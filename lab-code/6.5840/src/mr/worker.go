package mr

import (
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//log.Println("Worker starts")
	for {
		args := TaskApplyRequest{
			WorkerId: WorkerId(os.Getpid()),
		}
		reply := TaskAssignResponse{}

		ok := call("Coordinator.AssignTask", &args, &reply)
		if ok {
			//log.Println("reply is " + fmt.Sprintf("%v", reply))
			switch reply.TaskSignal {
			case Wait:
				//log.Println("No task available. Sleep 1 second.")
				time.Sleep(1 * time.Second)
			case RunMapTASK:
				fileNames := reply.MapTask.run(mapf)
				taskFinishedRequest := TaskFinishedRequest{
					TaskType:      MapTaskType,
					WorkerId:      WorkerId(os.Getpid()),
					TaskId:        TaskId(reply.MapTask.Id),
					TaskStartTime: reply.MapTask.StartTime,
					CommitFiles:   fileNames,
				}
				markReply := TaskAssignResponse{}
				//log.Println("Send map task finish request to coordinator " + fmt.Sprintf("%v", taskFinishedRequest))
				call("Coordinator.MarkTaskAsFinished", &taskFinishedRequest, &markReply)
			case RunReduceTask:
				fileNames := reply.ReduceTask.run(reducef)
				taskFinishedRequest := TaskFinishedRequest{
					TaskType:      ReduceTaskType,
					WorkerId:      WorkerId(os.Getpid()),
					TaskId:        TaskId(reply.ReduceTask.Id),
					TaskStartTime: reply.ReduceTask.StartTime,
					CommitFiles:   fileNames,
				}
				markReply := TaskAssignResponse{}
				//log.Println("Send reduce task finish request to coordinator " + fmt.Sprintf("%v", taskFinishedRequest))
				call("Coordinator.MarkTaskAsFinished", &taskFinishedRequest, &markReply)
			case Exit:
				//log.Println("worker exits")
				return
			}
		} else {
			panic("RPC call gets error response")
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

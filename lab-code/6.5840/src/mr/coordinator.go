package mr

import (
	"log"
	"strings"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//type Coordinator struct {
//	// Your definitions here.
//
//}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.stage == CompletedStage
}

// AssignTask
// it seems that the assigning task logic can be more abstract and elegant
// need to merge the duplicate code
func (c *Coordinator) AssignTask(args *TaskApplyRequest, reply *TaskAssignResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch c.stage {
	case InitStage:
		reply.TaskSignal = Wait
	case MapStage:
		reply.TaskSignal = RunMapTASK
		if len(c.mapTasks) > 0 {
			// assign a task to worker, then remove the task
			reply.MapTask = c.mapTasks[0]
			c.mapTasks = c.mapTasks[1:]
			// set start time
			reply.MapTask.StartTime = time.Now()
			// register the task in monitor
			c.mapTasksMonitor[TaskId(reply.MapTask.Id)] = MapTaskTuple{args.WorkerId, reply.MapTask}
		} else {
			reply.TaskSignal = Wait
			if len(c.mapTasksMonitor) > 0 {
				// remove all timeout tasks from monitor
				now := time.Now()
				var timeoutTasks []TaskId
				for id, tuple := range c.mapTasksMonitor {
					if tuple.MapTask.StartTime.Add(c.timeout).Before(now) {
						c.mapTasks = append(c.mapTasks, tuple.MapTask)
						timeoutTasks = append(timeoutTasks, id)
					}
				}
				for _, id := range timeoutTasks {
					delete(c.mapTasksMonitor, id)
					log.Printf("remove timeout map task %d from monitor", id)
				}
			}
		}
	case ReduceStage:
		reply.TaskSignal = RunReduceTask
		if len(c.reduceTasks) > 0 {
			reply.ReduceTask = c.reduceTasks[0]
			c.reduceTasks = c.reduceTasks[1:]
			reply.ReduceTask.StartTime = time.Now()
			c.reduceTasksMonitor[TaskId(reply.ReduceTask.Id)] = ReduceTaskTuple{args.WorkerId, reply.ReduceTask}
			//log.Println("assign a reduce task, " + strconv.Itoa(len(c.reduceTasks)) + " tasks left")
		} else {
			reply.TaskSignal = Wait
			if len(c.reduceTasksMonitor) > 0 {
				// remove all timeout tasks from monitor
				now := time.Now()
				var timeoutTasks []TaskId
				for id, tuple := range c.reduceTasksMonitor {
					if tuple.ReduceTask.StartTime.Add(c.timeout).Before(now) {
						c.reduceTasks = append(c.reduceTasks, tuple.ReduceTask)
						timeoutTasks = append(timeoutTasks, id)
					}
				}
				for _, id := range timeoutTasks {
					delete(c.reduceTasksMonitor, id)
					log.Printf("remove timeout reduce task %d from monitor", id)
				}
			}
		}
	case CompletedStage:
		reply.TaskSignal = Exit
	}

	return nil
}

func (c *Coordinator) MarkTaskAsFinished(args *TaskFinishedRequest, reply *TaskAssignResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	// check whether the task is timeout
	if args.TaskStartTime.Add(c.timeout).After(now) {
		// check whether the task is till owned by the related workerId
		// if so, remove the task from the monitor & rename the commit files
		// if not, delete all the commit files
		taskId := args.TaskId
		switch args.TaskType {
		case MapTaskType:
			tuple, ok := c.mapTasksMonitor[taskId]
			if ok && args.WorkerId == tuple.WorkerId {
				delete(c.mapTasksMonitor, taskId)
			}
		case ReduceTaskType:
			tuple, ok := c.reduceTasksMonitor[taskId]
			if ok && args.WorkerId == tuple.WorkerId {
				delete(c.reduceTasksMonitor, taskId)
			}
		}
		// commit tmp files
		for _, fileName := range args.CommitFiles {
			nameArray := strings.Split(fileName, "-")
			length := len(nameArray)
			if length > 1 {
				nameArray = nameArray[0 : length-1]
				newFileName := strings.Join(nameArray, "-")
				os.Rename(fileName, newFileName)
			}
		}

		// mark task as finished
		c.waitGroup.Done()
	} else {
		// if the worker is timeout, delete the commit files
		// do not need to remove the task from monitor to tasks array
		// as we have timeout task check in Assigning task function
		for _, fileName := range args.CommitFiles {
			DeleteFile(fileName)
		}
	}

	return nil
}

func (c *Coordinator) GenerateMapTasks(files []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for index, fileName := range files {
		c.mapTasks = append(c.mapTasks, MapTask{CommonFields{
			Id:         index,
			InputPath:  fileName,
			OutputPath: c.temporaryPath,
			ReduceNum:  c.reduceNum,
		}})
	}
	c.stage = MapStage
}

func (c *Coordinator) GenerateReduceTasks() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i := 0; i < c.reduceNum; i += 1 {
		c.reduceTasks = append(c.reduceTasks, ReduceTask{CommonFields{
			Id:         i,
			InputPath:  c.temporaryPath,
			OutputPath: c.finalPath,
			ReduceNum:  c.reduceNum,
		}})
	}
	c.stage = ReduceStage
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapNum:             len(files),
		reduceNum:          nReduce,
		temporaryPath:      "mr-lab1",
		finalPath:          "mr-out",
		mapTasks:           []MapTask{},
		reduceTasks:        []ReduceTask{},
		mapTasksMonitor:    make(map[TaskId]MapTaskTuple),
		reduceTasksMonitor: make(map[TaskId]ReduceTaskTuple),
		timeout:            10 * time.Second,
		stage:              InitStage,
	}

	c.server()

	// generate map tasks
	// when assign a task to a worker, assign StartTime with current time
	c.GenerateMapTasks(files)

	// wait for all map task finished
	c.WaitAllTasksFinished(c.mapNum)

	// make sure all map tasks have been finished
	CheckAllStageTasksFinished(len(c.mapTasks), len(c.mapTasksMonitor), "Map stage failed.")

	// generate reduce tasks
	c.GenerateReduceTasks()

	c.WaitAllTasksFinished(c.reduceNum)

	CheckAllStageTasksFinished(len(c.reduceTasks), len(c.reduceTasksMonitor), "Reduce stage failed.")

	// clean temporary files
	c.CleanIntermediateFiles()

	// mark job as finished
	c.stage = CompletedStage

	return &c
}

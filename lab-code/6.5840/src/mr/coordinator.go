package mr

import (
	"encoding/gob"
	"log"
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

func (c *Coordinator) TaskAssignHandler(args *TaskApplyRequest, reply *TaskAssignResponse) {
	if len(c.tasks) > 0 {
		reply.Task = c.tasks[0]
		reply.Task.StartTime = time.Now()
		c.tasks = c.tasks[1:]
		c.tasksMonitor[reply.Task.TaskId] = TaskTuple{args.WorkerId, reply.Task}
	} else {
		reply.TaskSignal = Wait
		now := time.Now()
		var timeoutTasks []TaskId
		for id, taskTuple := range c.tasksMonitor {
			if taskTuple.Task.StartTime.Add(c.timeout).Before(now) {
				c.tasks = append(c.tasks, taskTuple.Task)
				timeoutTasks = append(timeoutTasks, id)
			}
		}
		for _, id := range timeoutTasks {
			delete(c.tasksMonitor, id)
			log.Printf("remove timeout task %d from monitor at %v stage", id, c.stage)
		}
	}
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
		c.TaskAssignHandler(args, reply)
	case ReduceStage:
		reply.TaskSignal = RunReduceTask
		c.TaskAssignHandler(args, reply)
	case CompletedStage:
		reply.TaskSignal = Exit
	}

	return nil
}

func (c *Coordinator) MarkTaskAsFinished(args *TaskFinishedRequest, reply *TaskAssignResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	taskId := args.TaskId
	taskTuple, ok := c.tasksMonitor[taskId]
	now := time.Now()
	// check whether the task is timeout
	if ok && args.WorkerId == taskTuple.WorkerId && args.TaskStartTime.Add(c.timeout).After(now) {
		// check whether the task is till owned by the related workerId
		// if so, remove the task from the monitor & rename the commit files
		// if not, delete all the commit files
		delete(c.tasksMonitor, taskId)
		RenameFiles(args.CommitFiles)
		c.waitGroup.Done()
	} else {
		// if the worker is timeout, delete the commit files
		// do not need to remove the task from monitor to tasks array
		// as we have timeout task check in Assigning task function
		DeleteFiles(args.CommitFiles)
	}

	return nil
}

func (c *Coordinator) GenerateMapTasks(files []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for index, fileName := range files {
		c.tasks = append(c.tasks, Task{
			TaskId:     TaskId(index),
			InputPath:  fileName,
			OutputPath: c.temporaryPath,
			ReduceNum:  c.reduceNum,
		})
	}
	c.stage = MapStage
}

func (c *Coordinator) GenerateReduceTasks() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i := 0; i < c.reduceNum; i += 1 {
		c.tasks = append(c.tasks, Task{
			TaskId:     TaskId(i),
			InputPath:  c.temporaryPath,
			OutputPath: c.finalPath,
			ReduceNum:  c.reduceNum,
		})
	}
	c.stage = ReduceStage
}

func (c *Coordinator) CheckAllStageTasksFinished() {
	if len(c.tasks) != 0 {
		log.Fatalf("There are still %d tasks left to be processed. Fail at %v stage.", len(c.tasks), c.stage)
	}
	if len(c.tasksMonitor) != 0 {
		log.Fatalf("There are still %d tasks left in monitor. Fail at %v stage.", len(c.tasksMonitor), c.stage)
	}
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapNum:        len(files),
		reduceNum:     nReduce,
		temporaryPath: "mr-lab1",
		finalPath:     "mr-out",
		tasks:         []Task{},
		tasksMonitor:  make(map[TaskId]TaskTuple),
		timeout:       10 * time.Second,
		stage:         InitStage,
	}

	gob.Register(Task{})

	c.server()

	// generate map tasks
	// when assign a task to a worker, assign StartTime with current time
	c.GenerateMapTasks(files)

	// wait for all map task finished
	c.WaitAllTasksFinished(c.mapNum)

	// make sure all map tasks have been finished
	c.CheckAllStageTasksFinished()

	// generate reduce tasks
	c.GenerateReduceTasks()

	c.WaitAllTasksFinished(c.reduceNum)

	c.CheckAllStageTasksFinished()

	// clean temporary files
	c.CleanIntermediateFiles()

	// mark job as finished
	c.stage = CompletedStage

	return &c
}

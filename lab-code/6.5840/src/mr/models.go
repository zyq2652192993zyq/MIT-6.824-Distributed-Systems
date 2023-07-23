package mr

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type TaskType string
type Stage string
type WorkerId int
type TaskId int
type TaskSignal string
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	MapTaskType    TaskType = "MapTask"
	ReduceTaskType          = "ReduceTask"
)

const (
	MapStage       Stage = "MapStage"
	ReduceStage          = "ReduceStage"
	CompletedStage       = "CompletedStage"
	InitStage            = "InitStage"
)

const (
	Exit          TaskSignal = "EXIT"
	Wait                     = "WAIT"
	RunMapTASK               = "RUN_MAP_TASK"
	RunReduceTask            = "RUN_REDUCE_TASK"
)

type CommonFields struct {
	Id         int
	InputPath  string
	OutputPath string
	StartTime  time.Time
	ReduceNum  int
}

type MapTask struct {
	CommonFields
}

type ReduceTask struct {
	CommonFields
}

type MapTaskTuple struct {
	WorkerId WorkerId
	MapTask  MapTask
}

type ReduceTaskTuple struct {
	WorkerId   WorkerId
	ReduceTask ReduceTask
}

type Coordinator struct {
	mapNum             int
	reduceNum          int
	temporaryPath      string
	finalPath          string
	mapTasks           []MapTask
	reduceTasks        []ReduceTask
	mutex              sync.Mutex
	waitGroup          sync.WaitGroup
	mapTasksMonitor    map[TaskId]MapTaskTuple
	reduceTasksMonitor map[TaskId]ReduceTaskTuple
	timeout            time.Duration
	stage              Stage
}

func (t *MapTask) run(mapf func(string, string) []KeyValue) []string {
	// read file: open & read
	contents := ReadFileAsString(t.InputPath)

	// distribute KV to tmp reduce file
	keyValueArray := mapf(t.InputPath, contents)
	reduceIdMapToKvPair := make(map[int][]KeyValue)
	for _, kv := range keyValueArray {
		reduceId := ihash(kv.Key) % t.ReduceNum
		reduceIdMapToKvPair[reduceId] = append(reduceIdMapToKvPair[reduceId], kv)
	}

	// write each kv array to tmp files.
	// file format: [tmp path]-[reduceId]-[mapTaskIndex]-[pid]tmp.
	// The file format is used to avoid files covering each other when a task timeout
	var mapTaskDumpFiles []string
	for reduceId, kvArray := range reduceIdMapToKvPair {
		tmpFileName := fmt.Sprintf("%s-%d-%d-%dtmp", t.OutputPath, reduceId, t.Id, os.Getpid())
		WriteKeyValueToFile(kvArray, tmpFileName)
		mapTaskDumpFiles = append(mapTaskDumpFiles, tmpFileName)
	}

	// return all the tmp files' name for commit
	return mapTaskDumpFiles
}

func (t *ReduceTask) run(reducef func(string, []string) string) []string {
	var contents string
	// read all files has prefix mr-lab1-[reduceId]-*
	// Note: files with `tmp` suffix should be ignored
	prefix := fmt.Sprintf("%s-%d", t.InputPath, t.Id)
	files, _ := os.ReadDir("./")
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), prefix) && !strings.HasSuffix(file.Name(), "tmp") {
			contents += ReadFileAsString(file.Name())
		}
	}

	// reduce logic:
	// extract all KV pair to intermediate array
	var intermediate []KeyValue
	for _, kvStr := range strings.Split(contents, "\n") {
		kvArr := strings.Split(kvStr, " ")
		if len(kvArr) < 2 {
			continue
		}
		intermediate = append(intermediate, KeyValue{kvArr[0], kvArr[1]})
	}

	// sort the array by Key
	sort.Sort(ByKey(intermediate))

	// format of output reduce results: [mr-out]-[reduceId]
	outputFileName := fmt.Sprintf("%s-%d-%dtmp", t.OutputPath, t.Id, os.Getpid())
	var reduceOutputKvArray []KeyValue

	// get the reduce output KV: [Key] [reducef result]
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		count := reducef(intermediate[i].Key, values)
		reduceOutputKvArray = append(reduceOutputKvArray, KeyValue{intermediate[i].Key, count})

		i = j
	}
	WriteKeyValueToFile(reduceOutputKvArray, outputFileName)

	// return tmp files' name for commit
	return []string{outputFileName}
}

type TaskFinishedRequest struct {
	TaskType      TaskType
	WorkerId      WorkerId
	TaskId        TaskId
	TaskStartTime time.Time
	CommitFiles   []string
}

type TaskApplyRequest struct {
	WorkerId WorkerId
}

type TaskAssignResponse struct {
	MapTask    MapTask
	ReduceTask ReduceTask
	TaskSignal TaskSignal
}

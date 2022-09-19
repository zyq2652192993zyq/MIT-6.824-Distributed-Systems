# Lab 1: MapReduce

[Link](http://nil.csail.mit.edu/6.824/2021/labs/lab-mr.html)

Reference:

* [Go语言命名规范](https://www.cnblogs.com/rickiyang/p/11074174.html)
* [Go插件plugin教程](https://mojotv.cn/go/golang-plugin-tutorial)
* https://github.com/Anarion-zuo/AnBlogs/blob/master/6.824/lab1-mapreduce.md

```bash
git clone git://g.csail.mit.edu/6.824-golabs-2021 6.824
```

if you meet error `timeout command not available on mac os`, then you can
```shell
brew install coreutils
```

## Design Doc
* The whole process can be defined like the following:
```
file_1, file_2, ..., file_m => map => reduce => end
```

Firstly I define a basic Task

```go
type Task struct {
	processId int
	deadline  int
	timeout   int
	taskId    string
}
```

`taskId` is compsed by `task type` + `process id`.

Then we have two tasks named `MapTak` and `ReduceTask`.

```go
type MapTask struct {
	taskInfo     Task
	filename     string
	reduceNum    int
	outputPrefix string
	mapId        int
}

type ReduceTask struct {
	taskInfo    Task
	inputPrefix string
	reduceId    int
}
```

These two tasks all have a method `execute`, so that we can call the method with same name to run the task.

### Map stage

In the map stage, suppose we have `m` files. Then we need to generate `m` map tasks. Each map task will read the file and execute the map function. After that, the task will dump the output to the temparary file with format `mr-tmp/mr-<mapId>-<reduceId>`

. Finally the task will call a method to notify the coordinator that it has finished the task. 

Well when the coordinator receieve the signal from a map task. It should first check whether the task is valid. It means maybe the map task’s running time exceeds the timeout. 

The behavior seems like a barrier. Suppose we have a barrier which has a capacity of `m`. If all map tasks finished. Then the barrier disappears and we step into the next stage (reduce stage).

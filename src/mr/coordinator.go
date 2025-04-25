package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type time_t int64

var mu sync.Mutex // 声明一个互斥锁

type TaskInfo struct {
	taskId         int
	taskType       TaskType
	taskInputFile  string
	taskOutputFile string
	beginTime      time_t
	done           bool
}

// func newMapTask(task_type TaskType, ) TaskInfo {
// 	return TaskInfo{
// 		taskId: ,
// 	}
// }

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	mapTaskMap    map[int]*TaskInfo
	reduceTaskMap map[int]*TaskInfo
}

func getUndoneTask(taskMap map[int]*TaskInfo) *TaskInfo {
	var nextTask *TaskInfo
	for _, task := range taskMap {
		if !task.done && task.beginTime == 0 {
			nextTask = task
			break
		}
	}
	// start timer
	if nextTask != nil {
		nextTask.beginTime = time_t(time.Now().Unix())
	}
	return nextTask
}

func (c *Coordinator) CheckAllJobDone() bool {
	for _, task := range c.mapTaskMap {
		if !task.done {
			return false
		}
	}
	for _, task := range c.reduceTaskMap {
		if !task.done {
			return false
		}
	}
	return true
}

func (c *Coordinator) UpdateJob(taskType TaskType, taskId int) {
	if taskType == MAP {
		c.reduceTaskMap[taskId].done = true
		return
	} else if taskType == REDUCE {
		c.mapTaskMap[taskId].done = true
		return
	}
	panic("Invalid Task")
}

func (c *Coordinator) GetNextMapTask() *TaskInfo {

	nextMapTask := getUndoneTask(c.mapTaskMap)
	if nextMapTask != nil {
		return nextMapTask
	} else {
		return getUndoneTask(c.reduceTaskMap)
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(request *TaskRequest, reply *TaskReply) error {
	mu.Lock()
	if request.CurrentStatus == DONE_MAPPING {
		prevTaskId := request.PrevTask.TaskId
		c.mapTaskMap[prevTaskId].done = true
		for _, task := range c.mapTaskMap {
			fmt.Println("Task Map", task)
		}
	} else if request.CurrentStatus == DONE_REDUCING {
		c.reduceTaskMap[request.PrevTask.TaskId].done = true
		for _, task := range c.reduceTaskMap {
			fmt.Println("Task Map", task)
		}
	} else { // for new assigned task
		// update taskMap
		nextMapTask := c.GetNextMapTask()

		if nextMapTask != nil { // assign map task first
			reply.NReduce = c.nReduce
			reply.TaskType = nextMapTask.taskType
			reply.MapInputFile = nextMapTask.taskInputFile
			reply.TaskId = nextMapTask.taskId
		}
	}
	mu.Unlock()
	fmt.Println("Returning Reply:", reply)
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.CheckAllJobDone() == true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("Files:", files)

	var textFiles []string

	for _, file := range files {
		ext := filepath.Ext(file)
		if ext != ".so" {
			textFiles = append(textFiles, file)
		}
	}

	fmt.Println("Text Files:", textFiles)
	fmt.Println("N_REDUCE: ", nReduce)
	c := Coordinator{
		nReduce:       nReduce,
		mapTaskMap:    make(map[int]*TaskInfo),
		reduceTaskMap: make(map[int]*TaskInfo),
	}

	for idx, file := range textFiles {
		var newTask TaskInfo
		newTask.taskId = idx
		newTask.taskType = MAP
		newTask.taskInputFile = file
		c.mapTaskMap[idx] = &newTask
		fmt.Println("mapTaskMap insert: ", newTask)
	}

	for i := range nReduce {
		var newTask TaskInfo
		newTask.taskId = i
		newTask.taskType = REDUCE
		c.reduceTaskMap[i] = &newTask
		fmt.Println("reduceTaskMap insert: ", newTask)
	}

	c.server()
	return &c
}

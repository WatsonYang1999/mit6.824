package mr

import (
	"errors"
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
	mu.Lock()
	defer mu.Unlock()
	var nextTask *TaskInfo
	for _, task := range taskMap {
		if !task.done && (task.beginTime == 0 || time_t(time.Now().Unix())-task.beginTime > 20) {
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

func (c *Coordinator) CheckAllMapDone() bool {
	mu.Lock()
	defer mu.Unlock()
	for _, task := range c.mapTaskMap {
		fmt.Println("[Coordinator] Task Map", task)
	}
	for _, task := range c.reduceTaskMap {
		fmt.Println("[Coordinator] Reduce Map", task)
	}
	for _, task := range c.mapTaskMap {
		if !task.done {
			return false
		}
	}
	return true
}

func fileExists(filename string) bool {
	// 使用 os.Stat 获取文件信息
	fmt.Println("[Coordinator] Looking for :", filename)
	_, err := os.Stat(filename)
	// 如果文件存在，不返回错误
	if err == nil {
		fmt.Println("[Coordinator] Found :", filename)
		return true
	}
	// 如果文件不存在，返回错误信息
	if os.IsNotExist(err) {
		return false
	}
	// 其他错误情况，返回 false
	return false
}

func (c *Coordinator) CheckAllIntermediate() bool {
	for _, i := range c.mapTaskMap {
		for j := range c.nReduce {
			intermediate := fmt.Sprintf("intermediate-%d-%d.json", i.taskId, j)
			if !fileExists(intermediate) {
				return false
			}
		}
	}

	return true
}

func (c *Coordinator) CheckAllJobDone() bool {
	mu.Lock()
	defer mu.Unlock()
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
	mu.Lock()
	defer mu.Unlock()
	if taskType == MAP {
		c.mapTaskMap[taskId].done = true

	} else if taskType == REDUCE {
		c.reduceTaskMap[taskId].done = true

	} else {
		panic("[Coordinator]  Invalid Task")
	}
	for _, task := range c.mapTaskMap {
		fmt.Println("Task Map", task)
	}
	for _, task := range c.reduceTaskMap {
		fmt.Println("Task Reduce", task)
	}
}

func (c *Coordinator) GetNextMapTask() *TaskInfo {
	nextMapTask := getUndoneTask(c.mapTaskMap)
	if nextMapTask != nil {
		return nextMapTask
	} else {
		if c.CheckAllMapDone() {
			return getUndoneTask(c.reduceTaskMap)
		}
	}
	return nil
}

func (c *Coordinator) Example(request *TaskRequest, reply *TaskReply) error {
	fmt.Println("[Coordinator] Receiving Request:", request)
	if request.CurrentStatus == DONE_MAPPING {
		c.UpdateJob(MAP, request.PrevTask.TaskId)
	} else if request.CurrentStatus == DONE_REDUCING {
		c.UpdateJob(REDUCE, request.PrevTask.TaskId)
	} else { // for new assigned task
		// update taskMap
		nextMapTask := c.GetNextMapTask()
		fmt.Println("Get Next Job is:", nextMapTask)
		if nextMapTask != nil { // assign map task first
			reply.NReduce = c.nReduce
			reply.TaskType = nextMapTask.taskType
			reply.MapInputFile = nextMapTask.taskInputFile
			reply.TaskId = nextMapTask.taskId
		} else {
			if(c.CheckAllJobDone()) {
				fmt.Println("[Coordinator] All Jobs Are Done", reply)
				return nil
			}
			return errors.New("[Coordinator]  Gotta Wait All Map is Done to Begin Reduce")
		}
	}
	fmt.Println("[Coordinator] Returning Reply:", reply)
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

package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type time_t int64

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
	mapTaskMap    map[int]*TaskInfo
	reduceTaskMap map[int]*TaskInfo
}

func getUndoneTask(taskMap map[int]*TaskInfo) *TaskInfo{
	for _, task := range taskMap {
		if !task.done && task.beginTime == 0 {
			return task
		} 
	}
	return nil
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
	// for finished request
	fmt.Println("Receive a new task request:", request, " reply:", reply)
	if request.CurrentStatus == DONE {

	} else { // for new assigned task
		now_time := time.Now().Unix()

		// update taskMap
		nextMapTask := c.GetNextMapTask()
		nextMapTask.beginTime = time_t(now_time)

		if nextMapTask != nil { // assign map task first
			// create reply
			reply.TaskType = MAP
			reply.MapInputFile = nextMapTask.taskInputFile
			reply.TaskId = nextMapTask.taskId
		}
	}
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
	for _, task := range c.mapTaskMap {
		if task.done != true {
			return false
		}
	}

	for _, task := range c.reduceTaskMap {
		if task.done != true {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	N_REDUCE = nReduce

	c := Coordinator{
		mapTaskMap:    make(map[int]*TaskInfo),
		reduceTaskMap: make(map[int]*TaskInfo),
	}

	for idx, file := range files {
		var newTask TaskInfo
		newTask.taskId = idx
		newTask.taskType = MAP
		newTask.taskInputFile = file
		c.mapTaskMap[idx] = &newTask
	}

	for i := range N_REDUCE {
		var newTask TaskInfo
		newTask.taskId = i
		c.reduceTaskMap[i] = &newTask
	}

	c.server()
	return &c
}

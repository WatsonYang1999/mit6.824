package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType string
type Status string

const (
	MAP TaskType = "DoMap"
	REDUCE TaskType = "DoReduce" 
	NONE TaskType = "None"
)

const (
	IDLE Status = "IDLE"
	FAILED Status = "FAILED"
	DONE_MAPPING Status = "DONE_MAPPING"
	DONE_REDUCING Status = "DONE_REDUCING"
)

// The data struture work use to apply for new task
type TaskRequest struct {
	CurrentStatus Status
	PrevTask *TaskReply
}

// The data structure coordinator send to worker to specify job
type TaskReply struct {
	NReduce int
	TaskId int
	TaskType TaskType
	MapInputFile string
	// MapOutputFile string
	// ReduceInputFile string
	// ReduceOutputFile string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func ServerSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
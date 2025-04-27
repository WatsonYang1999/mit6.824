package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var N_REDUCE int = -1

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var status Status = IDLE
var prevTask *TaskReply = nil

// Map functions return a slice of KeyValue.

// use ihash(key) % N_REDUCE to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DoMap(mid int, inputPath string, mapf func(string, string) []KeyValue) error {
	if mid < 0 {
		panic("输入不能小于 0")
	}

	intermediate := []KeyValue{}
	file, err := os.Open(inputPath)
	if err != nil {
		log.Fatalf("cannot open %v", inputPath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputPath)
	}
	file.Close()
	kva := mapf(inputPath, string(content))
	intermediate = append(intermediate, kva...)

	var intermediateFiles []*os.File
	var encoders []*json.Encoder
	// create N_REDUCE intermediate file handlers
	for rid := range N_REDUCE {
		fileName := fmt.Sprintf("tmp-intermediate-%d-%d.json", mid, rid)
		file, err := os.Create(fileName)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(file)
		intermediateFiles = append(intermediateFiles, file)
		encoders = append(encoders, enc)
	}

	for _, item := range intermediate {
		k := item.Key
		rid := ihash(k) % N_REDUCE
		encoders[rid].Encode(item)
	}

	for _, fileHandler := range intermediateFiles {
		fileHandler.Close()
	}
	for rid := range N_REDUCE {
		oldName := fmt.Sprintf("tmp-intermediate-%d-%d.json", mid, rid)
		newName := fmt.Sprintf("intermediate-%d-%d.json", mid, rid)
	
		err := os.Rename(oldName, newName)
		if err != nil {
			return fmt.Errorf("failed to rename file %s to %s: %v", oldName, newName, err)
		}
	}

	return nil
}

func DoReduce(rid int, reducef func(string, []string) string) error {
	if rid < 0 {
		panic("输入不能小于 0")
	}
	var intermediate []KeyValue
	mid := 0
	for {
		fileName := fmt.Sprintf("intermediate-%d-%d.json", mid, rid)
		file, err := os.Open(fileName)
		if err != nil {
			if mid < 8 {
				panic("some intermediate files are missing! : " + fileName)
			}
			fmt.Println(err)
			break
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		mid ++
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", rid)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := Apply()
		fmt.Println("Receiving a task:", reply)
		if reply.TaskType == MAP {
			DoMap(reply.TaskId, reply.MapInputFile, mapf)
		} else if reply.TaskType == REDUCE {
			DoReduce(reply.TaskId, reducef)
		} else {
			time.Sleep(time.Duration(500 * time.Millisecond))
			continue
		}
		Done()
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func Apply() TaskReply {

	// declare an argument structure.
	args := TaskRequest{
		CurrentStatus: IDLE,
	}

	reply := TaskReply{
		TaskType:     MAP,
		MapInputFile: "",
	}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	prevTask = &reply
	if ok {
		N_REDUCE = reply.NReduce
	} else {
		fmt.Println("call failed:", ok)
		reply.TaskType = NONE
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func Done() TaskReply {
	// declare an argument structure.
	args := TaskRequest{}
	if prevTask.TaskType == MAP {
		args.CurrentStatus = DONE_MAPPING
	} else {
		args.CurrentStatus = DONE_REDUCING
	}
	args.PrevTask = prevTask
	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskReply{
	}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Println("Task Done:", prevTask)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

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

// Map functions return a slice of KeyValue.

// use ihash(key) % N_REDUCE to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DoMap(mid int, inputPath string, mapf func(string, string) []KeyValue) error {
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
	for rid := 0; rid < N_REDUCE; rid++ {
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
		fmt.Println("hashed rid: ", rid)
		encoders[rid].Encode(item)
	}

	for _, fileHandler := range intermediateFiles {
		fileHandler.Close()
	}
	return nil
}

func DoReduce(rid int, reducef func(string, []string) string) error {
	var intermediate []KeyValue
	for i := 0; i < N_REDUCE; i++ {
		fileName := fmt.Sprintf("tmp-intermediate-%d-%d.json", i, rid)
		file, err := os.Create(fileName)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			  break
			}
			intermediate = append(intermediate, kv)
		}
	}


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
	reply := CallExample()
	fmt.Println("Receiving a %s task", reply.TaskType)
	if reply.TaskType == MAP {
		DoMap(reply.TaskId, reply.MapInputFile, mapf)
	} else if reply.TaskType == REDUCE {
		DoReduce(reply.TaskId, reducef)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() TaskReply {

	// declare an argument structure.
	args := TaskRequest{
		CurrentStatus: IDLE,
	}

	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskReply{
		TaskId:       -1,
		TaskType:     MAP,
		MapInputFile: "",
	}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
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

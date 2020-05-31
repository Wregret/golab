package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(mapf func(string, string) []KeyValue, filename string, nReduce int, idx int) {
	// Read input files
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// Do user-defined map function
	kva := mapf(filename, string(content))
	// Make a json encoder list, bind them to temp files
	encoders := make([]*json.Encoder, nReduce)
	tempFiles := make(map[string]string)
	openFiles := make([]*os.File, 0)
	for i := range encoders {
		intermediateFilename := "mr-" + strconv.Itoa(idx) + "-" + strconv.Itoa(i)
		tempFile, err := ioutil.TempFile(".", intermediateFilename)
		if err != nil {
			log.Fatalf("%s %s", err, "Fail to create map temp file")
		}
		openFiles = append(openFiles, tempFile)
		tempFiles[tempFile.Name()] = intermediateFilename
		encoders[i] = json.NewEncoder(tempFile)
	}
	// Encode json into temp files
	for _, kv := range kva {
		hashKey := ihash(kv.Key) % nReduce
		err := encoders[hashKey].Encode(&kv)
		if err != nil {
			log.Fatalf("%s %s", err, "Fail to kv pair")
		}
	}
	// Close the temp files
	for _, f := range openFiles {
		f.Close()
	}
	// Rename temp file as intermediate file
	for tempName, intermediateFilename := range tempFiles {
		err := os.Rename(tempName, intermediateFilename)
		if err != nil {
			log.Fatalf("%s, %s", err, "Fail to rename map temp file")
		}
	}
}

func doReduce(reducef func(string, []string) string, kvMap map[string][]string, nMap int, idx int) {
	// Collect all intermediate files which belong to reduce idx
	for i := 0; i < nMap; i++ {
		intermediateFilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(idx)
		intermediateFile, err := os.Open(intermediateFilename)
		if err != nil {
			log.Fatalf("%s %s", err, "Fail to open intermediate file")
		}
		decoder := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		intermediateFile.Close()
	}
	// Create temp file
	outputFilename := "mr-out-" + strconv.Itoa(idx)
	tempFile, err := ioutil.TempFile(".", outputFilename)
	if err != nil {
		log.Fatalf("%s %s", err, "Faile to create reduce temp file")
	}
	// Do user-defined reduce for each key and write the result into temp file
	for key, values := range kvMap {
		output := reducef(key, values)
		fmt.Fprintf(tempFile, "%v %v\n", key, output)
	}
	tempFile.Close()
	// Rename the temp file as output file
	err = os.Rename(tempFile.Name(), outputFilename)
	if err != nil {
		log.Fatalf("%s %s", err, "Fail to rename reduce temp file")
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		ata := AssignTaskArgs{}
		atr := AssignTaskReply{}
		if call("Master.AssignTask", &ata, &atr) {
			switch atr.TaskType {
			case MapTask:
				filename := atr.Filename
				nReduce := atr.R
				idx := atr.ID
				// do Map
				doMap(mapf, filename, nReduce, idx)
				// Call FinishTask
				fta := FinishTaskArgs{TaskType: MapTask, ID: idx}
				ftr := FinishTaskReply{}
				call("Master.FinishTask", &fta, &ftr)

			case ReduceTask:
				kvMap := make(map[string][]string)
				nMap := atr.M
				idx := atr.ID
				// do Reduce
				doReduce(reducef, kvMap, nMap, idx)
				// Call FinishTask
				fta := FinishTaskArgs{TaskType: ReduceTask, ID: idx}
				ftr := FinishTaskReply{}
				call("Master.FinishTask", &fta, &ftr)
			}
		} else {
			break
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

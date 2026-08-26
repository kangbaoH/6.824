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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//参数1 通信，参数2 3 分别是map和reduce，类似函数指针

	coordSockName = sockname //用来和coordinator通信

	// Your worker implementation here.

	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}
		if !call("Coordinator.RequestTask", &args, &reply) {
			fmt.Println("request task call failed.")
			return
		}

		switch reply.TaskType {
		case MapTask:
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			intermediates := mapf(filename, string(content))
			//输入文件

			//fmt.Printf("task id:%d, key count:%d\n", reply.TaskID, len(intermediates))
			buckets := make([][]KeyValue, reply.NReduce)
			for _, intermediate := range intermediates {
				reduceID := ihash(intermediate.Key) % reply.NReduce
				buckets[reduceID] = append(buckets[reduceID], intermediate)
			}

			for reduceID, bucket := range buckets {
				ofile, err := os.CreateTemp(".", "Tempfile*")
				if err != nil {
					fmt.Println("createtemp fail")
					return
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range bucket {
					err := enc.Encode(&kv)
					if err != nil {
						fmt.Println("Encode fail")
						return
					}
				}
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceID)
				ofile.Close()
				os.Rename(ofile.Name(), oname)
			}
			FinishArgs := TaskFinishArgs{}
			FinishReply := TaskFinishReply{}
			FinishArgs.TaskID = reply.TaskID
			FinishArgs.TaskType = reply.TaskType
			if !call("Coordinator.FinishTask", &FinishArgs, &FinishReply) {
				fmt.Println("map finish task call failed.")
			}

		case ReduceTask:
			var intermediates []KeyValue
			for i := 0; i < reply.NMap; i += 1 {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
				file, err := os.Open(filename)
				if err != nil {
					fmt.Println("reduce open file fail")
					return
				}

				dec := json.NewDecoder(file)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediates = append(intermediates, kv)
				}

				file.Close()
			}
			//fmt.Printf("task id:%d, key count:%d\n", reply.TaskID, len(intermediates))

			sort.Sort(ByKey(intermediates))

			ofile, _ := os.CreateTemp(".", "Tempfile*")

			i := 0
			for i < len(intermediates) {
				j := i + 1
				for j < len(intermediates) &&
					intermediates[j].Key == intermediates[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediates[k].Value)
				}
				output := reducef(intermediates[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediates[i].Key, output)

				i = j
			}

			ofile.Close()
			oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			err := os.Rename(ofile.Name(), oname)
			if err != nil {
				fmt.Println("reduce rename fail")
				return
			}

			FinishArgs := TaskFinishArgs{}
			FinishReply := TaskFinishReply{}
			FinishArgs.TaskID = reply.TaskID
			FinishArgs.TaskType = reply.TaskType
			if !call("Coordinator.FinishTask", &FinishArgs, &FinishReply) {
				fmt.Println("reduce finish task call fail")
			}

		case WaitTask:
			time.Sleep(time.Second)

		case ExitTask:
			return

		default:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName) //建立连接
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil { //发送请求
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

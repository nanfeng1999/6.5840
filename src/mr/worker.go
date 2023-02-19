package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := RequestTask()

		switch task.Type {
		case MAP:
			DPrintln("map task ", task.FileName)
			dealWithMapTask(&task, mapf)
			TaskDone(task.ID)
			break
		case REDUCE:
			DPrintln("reduce task")
			dealWithReduceTask(&task, reducef)
			TaskDone(task.ID)
			break
		case NO_TASK:
			break
		case QUIT:
			TaskDone(task.ID)
			time.Sleep(time.Second)
			os.Exit(0)
		default:
			DPrintln("unsupport task")
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func RequestTask() Task {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		DPrintf("get Task %s success\n", reply.Task.Type)
	} else {
		DPrintf("get Task failed!\n")
	}

	return reply.Task
}

func TaskDone(id int) {
	args := TaskDoneArgs{id}
	reply := TaskDoneReply{}

	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		DPrintf("send Task Done success\n")
	} else {
		DPrintf("send Task Done failed!\n")
	}

	return
}

func dealWithMapTask(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.FileName

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 读取文件内容
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// 执行map函数 得到kv对

	kva := mapf(filename, string(content))
	buckets := make(map[int][]KeyValue)
	nReduce := task.NReduce
	// 根据不同的key 放入不同的bucket
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		buckets[index] = append(buckets[index], kv)
	}

	// 把不同bucket的内容写入文件中去
	for i := 0; i < nReduce; i++ {
		rFile, err := os.Create(getTempFileName(task.ID, i))
		if err != nil {
			log.Println("create file fail!")
		}
		enc := json.NewEncoder(rFile)
		kvs := buckets[i]

		for _, kv := range kvs {
			if err := enc.Encode(kv); err != nil {
				log.Println("encode kv fail! err:", err)
			}
		}

	}
}

func dealWithReduceTask(task *Task, reducef func(string, []string) string) {
	nMap := task.NMap
	intermediate := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		rFile, err := os.Open(getTempFileName(i, task.ID))
		if err != nil {
			log.Println("open file fail!")
		}

		dec := json.NewDecoder(rFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	ofile, _ := os.Create(getOutputFileName(task.ID))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		// 第i个kv固定 j从i+1开始查找
		j := i + 1
		// 找到第一个和i对应kv的key不同的kv为止
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// 从i~j-1都是相同的key
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 这些相同key的kv对 放入reduce函数 得到输出
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// 写入文件中
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// 删除文件
	for i := 0; i < nMap; i++ {
		os.Remove(getTempFileName(i, task.ID))
	}

}

func getTempFileName(mapNumber int, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

func getOutputFileName(reduceNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceNumber)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	DPrintln(err)
	return false
}

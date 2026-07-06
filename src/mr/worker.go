package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
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

	for {
		reply, ok := requestTask()
		if !ok {
			// Coordinator is unreachable; it has probably exited because the
			// job is complete. Stop.
			return
		}

		switch reply.Type {
		case TaskMap:
			doMap(mapf, &reply)
			reportTask(TaskMap, reply.TaskID)
		case TaskReduce:
			doReduce(reducef, &reply)
			reportTask(TaskReduce, reply.TaskID)
		case TaskNone:
			// No task ready yet; wait briefly and ask again.
			time.Sleep(200 * time.Millisecond)
		case TaskExit:
			return
		}
	}
}

// doMap runs the map function on one input file and partitions the emitted
// key/value pairs into NReduce intermediate files named mr-<mapID>-<reduceID>.
// Each file is written to a temp file first and atomically renamed so that a
// crash mid-write never leaves a partial file that a reducer could read.
func doMap(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File)
	}
	file.Close()

	kva := mapf(reply.File, string(content))

	// Bucket the intermediate pairs by reduce task.
	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		buckets[r] = append(buckets[r], kv)
	}

	for r := 0; r < reply.NReduce; r++ {
		// Write to a uniquely-named temp file in the current directory (same
		// filesystem as the final file, so os.Rename is atomic and never fails
		// with EXDEV). We build the temp name deterministically from our PID
		// instead of using ioutil.TempFile, whose O_EXCL probing loop can hang
		// on the WSL2 9p filesystem.
		tmpName := fmt.Sprintf(".mr-map-tmp-%d-%d-%d", os.Getpid(), reply.TaskID, r)
		tmp, err := os.Create(tmpName)
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode kv: %v", err)
			}
		}
		tmp.Close()
		final := fmt.Sprintf("mr-%d-%d", reply.TaskID, r)
		if err := os.Rename(tmpName, final); err != nil {
			log.Fatalf("cannot rename %v -> %v: %v", tmpName, final, err)
		}
	}
}

// doReduce reads every intermediate file for this reduce bucket (one per map
// task), groups values by key, applies the reduce function, and writes the
// result to mr-out-<reduceID> atomically.
func doReduce(reducef func(string, []string) string, reply *RequestTaskReply) {
	var intermediate []KeyValue
	for m := 0; m < reply.NMap; m++ {
		name := fmt.Sprintf("mr-%d-%d", m, reply.TaskID)
		file, err := os.Open(name)
		if err != nil {
			// A missing intermediate file means the map output isn't there;
			// skip it (the coordinator guarantees all maps are done first, so
			// this should not normally happen, but be defensive).
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(byKey(intermediate))

	// See doMap: avoid ioutil.TempFile because its O_EXCL probing hangs on the
	// WSL2 9p filesystem. Use a deterministic per-PID temp name instead.
	tmpName := fmt.Sprintf(".mr-out-tmp-%d-%d", os.Getpid(), reply.TaskID)
	tmp, err := os.Create(tmpName)
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmp, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmp.Close()

	final := fmt.Sprintf("mr-out-%d", reply.TaskID)
	if err := os.Rename(tmpName, final); err != nil {
		log.Fatalf("cannot rename %v -> %v: %v", tmp.Name(), final, err)
	}
}

// requestTask asks the coordinator for the next task.
func requestTask() (RequestTaskReply, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	return reply, ok
}

// reportTask tells the coordinator a task finished.
func reportTask(t TaskType, id int) {
	args := ReportTaskArgs{Type: t, TaskID: id}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// Coordinator has likely exited; report failure so the worker can stop.
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}

package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

// task states inside the coordinator.
const (
	stateIdle = iota
	stateInProgress
	stateDone
)

// taskTimeout is how long the coordinator waits for a worker to finish a task
// before assuming it crashed and re-issuing the task to someone else.
const taskTimeout = 10 * time.Second

type taskInfo struct {
	state     int
	startTime time.Time
}

type Coordinator struct {
	mu sync.Mutex

	files   []string
	nMap    int
	nReduce int

	mapTasks    []taskInfo
	reduceTasks []taskInfo

	mapDone    int // count of completed map tasks
	reduceDone int // count of completed reduce tasks
}

// RequestTask hands out the next available map or reduce task.
// Map tasks are drained fully before any reduce task is offered, because
// every reducer needs the intermediate output of every mapper.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Phase 1: map tasks.
	if c.mapDone < c.nMap {
		for i := range c.mapTasks {
			t := &c.mapTasks[i]
			if t.state == stateIdle || (t.state == stateInProgress && now.Sub(t.startTime) > taskTimeout) {
				t.state = stateInProgress
				t.startTime = now
				reply.Type = TaskMap
				reply.TaskID = i
				reply.File = c.files[i]
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				return nil
			}
		}
		// All map tasks are assigned but not all finished; ask the worker to wait.
		reply.Type = TaskNone
		return nil
	}

	// Phase 2: reduce tasks.
	if c.reduceDone < c.nReduce {
		for i := range c.reduceTasks {
			t := &c.reduceTasks[i]
			if t.state == stateIdle || (t.state == stateInProgress && now.Sub(t.startTime) > taskTimeout) {
				t.state = stateInProgress
				t.startTime = now
				reply.Type = TaskReduce
				reply.TaskID = i
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				return nil
			}
		}
		reply.Type = TaskNone
		return nil
	}

	// Everything is done.
	reply.Type = TaskExit
	return nil
}

// ReportTask records the completion of a map or reduce task.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case TaskMap:
		t := &c.mapTasks[args.TaskID]
		if t.state != stateDone {
			t.state = stateDone
			c.mapDone++
		}
	case TaskReduce:
		t := &c.reduceTasks[args.TaskID]
		if t.state != stateDone {
			t.state = stateDone
			c.reduceDone++
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceDone == c.nReduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]taskInfo, len(files)),
		reduceTasks: make([]taskInfo, nReduce),
	}

	c.server()
	return &c
}

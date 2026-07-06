package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// TaskType distinguishes the kind of work the coordinator hands out.
type TaskType int

const (
	TaskNone   TaskType = iota // no task available yet, worker should wait
	TaskMap                    // a map task
	TaskReduce                 // a reduce task
	TaskExit                   // all work is done, worker should exit
)

// RequestTaskArgs is empty: a worker just asks "give me something to do".
type RequestTaskArgs struct {
}

// RequestTaskReply carries a task assignment from the coordinator.
type RequestTaskReply struct {
	Type    TaskType
	TaskID  int    // index of the map or reduce task
	File    string // input file for a map task
	NMap    int    // number of map tasks (== number of input files)
	NReduce int    // number of reduce buckets
}

// ReportTaskArgs tells the coordinator a task has finished.
type ReportTaskArgs struct {
	Type   TaskType
	TaskID int
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

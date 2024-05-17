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

// Add your RPC definitions here.
type Args1 struct{ Task_id int } // -1 for asking a task and 1 for telling
// coordinator that the task is done
type Reply1 struct {
	State int //0 for map, 1 for reduce, 2 for no tasks available currently,

	Files   []string // a slice of file titles
	Index   int      // This tells the index of the map or reduce task assigned to a worker
	NReduce int      // number of reduce tasks
	M       int      // number of map tasks
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

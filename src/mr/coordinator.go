package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu    sync.Mutex
	Files []string
	State int //0 for map, 1 for reduce, 2 for no tasks available currently,
	//3 for all jobs done
	M         int            //number of map tasks (which is the number of files)
	Map_Tasks map[string]int //record status of map tasks, 0 for unassigned, 1 for in progress
	//and 2 for finished
	Reduce_Tasks map[int]int //record status of reduce tasks, 0 for unassigned, 1 for in progress
	//and 2 for finished

	nReduce int // number of reduce tasks

	//These two maps keep track of when a map or reduce task is assigned
	//if later we find a worker takes too long to complete a task
	//then we have to assume that the worker has crushed and
	//reassign the task to another work asking for tasks.
	Map_Tasks_Start    map[string]time.Time
	Reduce_Tasks_Start map[int]time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {

//reply.Y = args.X + 1
//return nil
//}

func (c *Coordinator) Communicate(args *Args1, reply *Reply1) error {
	// assigning task_id: [0,M-1] to map tasks and [M,M+R-1] to reduce tasks
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task_id != -1 {

		if args.Task_id < c.M {
			c.Map_Tasks[c.Files[args.Task_id]] = 2
		} else {

			c.Reduce_Tasks[args.Task_id-c.M] = 2
		}
		return nil

	}

	//args.Task_id == -1, need to assign task
	//we first start with map tasks

	// We begin by assigning map tasks whenever there is one available

	for i, f := range c.Files {
		if c.Map_Tasks[f] == 0 {
			reply.State = 0
			reply.Files = c.Files
			reply.Index = i
			reply.M = c.M
			reply.NReduce = c.nReduce
			//update the status of this map task
			c.Map_Tasks[f] = 1

			//record the time when this task is assigned
			c.Map_Tasks_Start[f] = time.Now()

			return nil

		}
	}
	//if the above code block does not return, then it means
	//either all the map tasks have been assigned but not finished (either 1 or 2 in
	//Map_Tasks)
	//or all the map tasks have been done and we need to assign a reduce task
	//we need to check saparately

	// Moreover, we need to check all map tasks that are not finished
	// if 10s have passed since the assignment
	// we assume the old worker working on the task is dead and
	// assign this task to the new worker asking for task

	// Otherwise we reply with state 2 and let the worker wait.
	max_elapse := time.Second * 0
	max_index := -1
	for i, f := range c.Files {

		if c.Map_Tasks[f] == 1 {
			t := time.Now()
			time_elapse := t.Sub(c.Map_Tasks_Start[f])
			if time_elapse > max_elapse {
				max_elapse = time_elapse
				max_index = i
			}

		}

	}
	if max_index != -1 {
		if max_elapse > time.Second*10 {
			// re-assign the map task
			reply.State = 0
			reply.Files = c.Files
			reply.Index = max_index
			reply.M = c.M
			reply.NReduce = c.nReduce
			//update the status of this map task
			c.Map_Tasks[c.Files[max_index]] = 1

			//record the time when this task is assigned
			c.Map_Tasks_Start[c.Files[max_index]] = time.Now()

			return nil

		} else {
			reply.State = 2
			return nil
		}
	} else {
		// If max_index == -1, then there are no unfinished map tasks
		// and we can check for reduce tasks

		// First we assign reduce tasks whenever there is one available

		for j := 0; j < c.nReduce; j++ {

			if c.Reduce_Tasks[j] == 0 {

				reply.State = 1
				reply.Index = j
				reply.M = c.M
				reply.NReduce = c.nReduce
				c.Reduce_Tasks[j] = 1
				c.Reduce_Tasks_Start[j] = time.Now()

				return nil

			}

		}

		// If the above code doesn't return, then it means reduce
		// tasks are either completely done or with a few unfinished.
		// We need to check both cases.
		// In the second case, if it has been more than 10 seconds
		// since the assignment of the task, we assume the old worker
		// has died out and reassign the task to the new working asking for tasks.

		r_max_elapse := time.Second * 0
		r_max_index := -1
		for j := 0; j < c.nReduce; j++ {

			if c.Reduce_Tasks[j] == 1 {
				t1 := time.Now()

				time_elapse := t1.Sub(c.Reduce_Tasks_Start[j])
				if time_elapse > r_max_elapse {
					r_max_elapse = time_elapse
					r_max_index = j
				}

			}

		}

		if r_max_index != -1 {
			// If this index is not -1, then it means some reduce tasks are assigned
			// but unfinished

			if r_max_elapse > time.Second*10 {

				// reassign the task

				reply.State = 1
				reply.Index = r_max_index
				reply.M = c.M
				reply.NReduce = c.nReduce
				c.Reduce_Tasks[r_max_index] = 1
				c.Reduce_Tasks_Start[r_max_index] = time.Now()

				return nil

			} else {
				// else there is no tasks to assign, simply ask the worker to wait
				reply.State = 2
				return nil

			}

		} else {
			//r_max_index == -1, so all the reduce tasks have also been done
			//there is no task to be assigned, simply let the worker wait
			//until the coordinator is closed
			reply.State = 2
			return nil

		}

	}

}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	// We simply go through Map_Tasks and Reduce_Tasks to
	// check if everything is set to 2

	for _, file := range c.Files {
		if c.Map_Tasks[file] != 2 {

			ret = false
		}
	}

	for i := 0; i < c.nReduce; i++ {

		if c.Reduce_Tasks[i] != 2 {
			ret = false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.M = len(files)

	// Your code here.
	c.State = 0
	c.Files = files
	temp := make(map[string]int)
	for _, file := range files {

		temp[file] = 0
	}
	c.Map_Tasks = temp
	temp2 := make(map[int]int)
	for i := 0; i < nReduce; i++ {
		temp2[i] = 0
	}

	c.Reduce_Tasks = temp2
	c.nReduce = nReduce

	c.Map_Tasks_Start = make(map[string]time.Time)
	c.Reduce_Tasks_Start = make(map[int]time.Time)

	c.server()
	return &c
}

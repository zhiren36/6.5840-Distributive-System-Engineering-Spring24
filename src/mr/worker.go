package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// Periodically checks with the coordinator to see
	// if there is any task to be done
	// and if there is, whether the task is map or reduce

	for {
		args := Args1{}
		reply := Reply1{}
		args.Task_id = -1
		ok := call("Coordinator.Communicate", &args, &reply)
		if ok {

			//do the task based on value returned from communicate

			if reply.State == 0 {

				// Do the map task

				filename := reply.Files[reply.Index]
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				Map_output := mapf(filename, string(content))

				hash := make(map[int][]KeyValue)
				// compute the hash values and make separate groups
				for _, kva := range Map_output {

					num := ihash(kva.Key) % reply.NReduce

					hash[num] = append(hash[num], kva)

				}

				// Create correponding files to store intermediate results

				for i := 0; i < reply.NReduce; i++ {
					temp_file_name := []string{"mr", strconv.Itoa(reply.Index),
						strconv.Itoa(i)}

					temp_file_name1 := strings.Join(temp_file_name, "-")

					temp_file, _ := os.Create(temp_file_name1)
					enc := json.NewEncoder(temp_file)
					for _, kv := range hash[i] {
						err := enc.Encode(&kv)

						if err != nil {
							fmt.Println("Jason encoding error")
						}

					}

					temp_file.Close()

				}

				// Aftere the map task is done, need to contact the
				// coordinator again to inform that work is done.

				new_args := Args1{}
				new_args.Task_id = reply.Index
				new_reply := Reply1{}
				call("Coordinator.Communicate", &new_args, &new_reply)
			}

			if reply.State == 1 {
				// Do the reduce task

				// First, need to aggregate all the intermetate
				// files with the same reduce index

				// We do this by successively reading all the file contents
				// into a list

				intermediate := []KeyValue{}
				for i := 0; i < reply.M; i++ {
					temp_file_name := []string{"mr", strconv.Itoa(i),
						strconv.Itoa(reply.Index)}

					temp_file_name1 := strings.Join(temp_file_name, "-")
					temp_file, _ := os.Open(temp_file_name1)
					dec := json.NewDecoder(temp_file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}

					temp_file.Close()

				}

				sort.Sort(ByKey(intermediate))
				oname := []string{"mr", "out", strconv.Itoa(reply.Index)}
				oname1 := strings.Join(oname, "-")
				ofile, _ := os.Create(oname1)
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

				// call the coordinator to let it know the reduce task is done
				new_args := Args1{}
				new_args.Task_id = reply.Index + reply.M
				new_reply := Reply1{}
				call("Coordinator.Communicate", &new_args, &new_reply)

			}

			if reply.State == 2 {
				// No tasks currently available, just wait
				// and continue a new loop afterwards
				time.Sleep(time.Second * 5)
				continue

			}

		} else {
			// If the worker fails to communicate with the coordinator
			// we automatically assume that all the tasks have been done
			// and we simply exit.
			fmt.Println("All tasks have been finished and this worker process will terminate")
			os.Exit(0)

		}

	}
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

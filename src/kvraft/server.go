package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// this struct contains the put/get/append commands that
	// will be sent to raft's apply channel and stored in log
	Client_ID   int64  // client id
	Task_ID     int    // task id
	Op_command  string // Get, Put or Append
	Op_key      string
	Op_argument string // nil for a get operation

}

// type OP_Get func(string) string
// type OP_PutAndAppend func(string, string, string) string

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// A map that stores all Key/Value pairs, this is the database
	Data map[string]string

	// A map that stores a map from clientID to the last applied request id
	// from that clerk. Only Put and Append operations are recorded.
	Processed map[int64]int

	// For each index in the log, we have a channel that receives ApplyMsg's as
	// Raft peers form consensus
	WaitChan map[int]chan raft.ApplyMsg

	// A table that stores all the Get values to keep linearizability
	//GetTable map[int64]map[int]string
	GetTable map[int64]Get_Info // only stores latest Get reply for a client

	LogIndex int // latest log index whose command gets applied to state machine
}
type Get_Info struct {
	Latest_Task int
	Reply       string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

	// first we need to create the op and push it to raft
	// no argument for get operation
	op := Op{}
	op.Client_ID = args.Client_id
	op.Task_ID = args.Task_id
	op.Op_command = "Get"
	op.Op_key = args.Key
	// if already replied to the request, just reply with whatever
	// value we record
	// rep, ok := kv.GetTable[args.Client_id][args.Task_id]
	// if ok {
	// 	reply.Value = rep
	// 	reply.Err = OK
	// 	return
	// }
	kv.mu.Lock()
	rep, ok := kv.GetTable[args.Client_id]
	kv.mu.Unlock()
	if ok && args.Task_id == rep.Latest_Task {
		reply.Value = rep.Reply
		reply.Err = OK
		return
	}
	if ok && args.Task_id < rep.Latest_Task {
		fmt.Println("GuiGuShi, Get args.Task_id < rep.Latest_Task")
	}

	// push to raft
	//kv.mu.Unlock()
	index, _, isleader := kv.rf.Start(op)
	//kv.mu.Lock()

	// if not leader, simply return and do nothing
	if isleader == false {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for raft to reach agreement
	// first, we create a wait channel to receive ApplyMsg's after Raft forms consensus
	kv.mu.Lock()
	_, exist := kv.WaitChan[index]
	if exist {
		fmt.Println("Error, wait channel already exists")
	} else {
		kv.WaitChan[index] = make(chan raft.ApplyMsg, 1)
	}

	kv.mu.Unlock()

	select {

	case Response := <-kv.WaitChan[index]:
		// Raft peers have formed a concensus, we can apply to state machine
		// Need to check if it's the same operation that has already been applied before
		temp := Response.Command.(Op)
		// just a sanity check
		if temp.Client_ID != op.Client_ID || temp.Task_ID != op.Task_ID || temp.Op_command != "Get" {
			fmt.Println("GUIGUSHI, commited op has different info")
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		a, e := kv.GetTable[temp.Client_ID]
		if e {

			reply.Value = a.Reply
			reply.Err = OK
		} else {
			fmt.Println("GuiGuShi Entry Disappeared in GetTable")
			reply.Err = ErrWrongLeader
		}

		kv.mu.Unlock()
		//fmt.Println("Get, Err, key, value, clien_id, taskID ", reply.Err, args.Key, " | ", reply.Value, args.Client_id, args.Task_id)

	case <-time.After(time.Millisecond * 500):
		//fmt.Println("server timeout when handling Get", kv.me)
		// the client will keep retrying
		reply.Err = ErrWrongLeader

	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// first we need to create the op and push it to raft
	op := Op{}
	op.Client_ID = args.Client_id
	op.Task_ID = args.Task_id
	op.Op_command = "Put"
	op.Op_key = args.Key
	op.Op_argument = args.Value

	// push to raft
	index, _, isleader := kv.rf.Start(op)

	// if not leader, simply return and do nothing
	if isleader == false {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for raft to reach agreement
	// first, we create a wait channel to receive ApplyMsg's after Raft forms consensus
	kv.mu.Lock()
	_, exist := kv.WaitChan[index]
	if exist {
		fmt.Println("Error, wait channel already exists")
	} else {
		kv.WaitChan[index] = make(chan raft.ApplyMsg, 1)
	}
	kv.mu.Unlock()

	// current_time := time.Now()
	// timeout := time.Millisecond * 500
	// for {

	// 	if time.Since(current_time) > timeout {

	// 		//fmt.Println("server timeout when handling Put", kv.me)
	// 		// the client will keep retrying a different server
	// 		reply.Err = ErrWrongLeader
	// 		return

	// 	} else {
	// 		Response := <-kv.WaitChan[index]
	// 		// Raft peers have formed a concensus, we can apply to state machine
	// 		// Need to check if it's the same operation that has already been applied before
	// 		temp := Response.Command.(Op)

	// 		// a sanity check

	// 		if temp.Client_ID != op.Client_ID || temp.Task_ID != op.Task_ID {
	// 			fmt.Println("GUIGUSHI")
	// 			reply.Err = ErrWrongLeader
	// 			return
	// 		}
	// 		reply.Err = OK
	// 		return
	// 		//fmt.Println("Put, Err, key, value ", reply.Err, temp.Op_key, "|", temp.Op_argument)

	// 	}

	// }

	select {

	case Response := <-kv.WaitChan[index]:
		// Raft peers have formed a concensus, we can apply to state machine
		// Need to check if it's the same operation that has already been applied before
		temp := Response.Command.(Op)

		// a sanity check

		if temp.Client_ID != op.Client_ID || temp.Task_ID != op.Task_ID {
			fmt.Println("GUIGUSHI")
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		//fmt.Println("Put, Err, key, value, clientID, taskID ", reply.Err, temp.Op_key, "|", temp.Op_argument, args.Client_id, args.Task_id)

	case <-time.After(time.Millisecond * 500):
		//fmt.Println("server timeout when handling Put", kv.me)
		// the client will keep retrying
		reply.Err = ErrWrongLeader

	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	// first we need to create the op and push it to raft
	op := Op{}
	op.Client_ID = args.Client_id
	op.Task_ID = args.Task_id
	op.Op_command = "Append"
	op.Op_key = args.Key
	op.Op_argument = args.Value

	// push to raft
	index, _, isleader := kv.rf.Start(op)

	// if not leader, simply return and do nothing
	if isleader == false {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for raft to reach agreement
	// first, we create a wait channel to receive ApplyMsg's after Raft forms consensus
	kv.mu.Lock()
	_, exist := kv.WaitChan[index]
	if exist {
		fmt.Println("Error, wait channel already exists")
	} else {
		kv.WaitChan[index] = make(chan raft.ApplyMsg, 1)
	}
	kv.mu.Unlock()

	// current_time := time.Now()
	// timeout := time.Millisecond * 500
	// for {
	// 	if time.Since(current_time) > timeout {
	// 		fmt.Println("server timeout when handling Append", kv.me)
	// 		// the client will keep retrying
	// 		reply.Err = ErrWrongLeader
	// 		return

	// 	} else {
	// 		Response := <-kv.WaitChan[index]
	// 		// Raft peers have formed a concensus, we can apply to state machine
	// 		// Need to check if it's the same operation that has already been applied before
	// 		temp := Response.Command.(Op)

	// 		if temp.Client_ID != op.Client_ID || temp.Task_ID != op.Task_ID || index != Response.CommandIndex {
	// 			fmt.Println("GUIGUSHI")
	// 			reply.Err = ErrWrongLeader
	// 			return
	// 		}
	// 		reply.Err = OK
	// 		return
	// 		//fmt.Println("Append, Err, key, value ", reply.Err, temp.Op_key, "|", temp.Op_argument)

	// 	}

	// }

	select {

	case Response := <-kv.WaitChan[index]:
		// Raft peers have formed a concensus, we can apply to state machine
		// Need to check if it's the same operation that has already been applied before
		temp := Response.Command.(Op)

		if temp.Client_ID != op.Client_ID || temp.Task_ID != op.Task_ID || index != Response.CommandIndex {
			fmt.Println("GUIGUSHI")
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		//fmt.Println("Append, Err, key, value, clientID, TaskID ", reply.Err, temp.Op_key, "|", temp.Op_argument, args.Client_id, args.Task_id)

	case <-time.After(time.Millisecond * 250):
		//fmt.Println("server timeout when handling Append", kv.me)
		// the client will keep retrying
		reply.Err = ErrWrongLeader

	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// func (kv *KVServer) isLeader(args *isLeaderArgs, reply *isLeaderReply) {
// 	// check whether the raft associated with kv thinks it is the leader
// 	_, isLeader := kv.rf.GetState()
// 	reply.isLeader = isLeader
// }

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, kv.persister, kv.applyCh)
	kv.WaitChan = make(map[int]chan raft.ApplyMsg)
	kv.Processed = make(map[int64]int)
	kv.Data = make(map[string]string)
	//kv.GetTable = make(map[int64]map[int]string)
	kv.GetTable = make(map[int64]Get_Info)

	snapshot_data := kv.persister.ReadSnapshot()

	if len(snapshot_data) > 0 {
		r := bytes.NewBuffer(snapshot_data)
		d := labgob.NewDecoder(r)
		var Data map[string]string
		var Processed map[int64]int
		//var GetTable map[int64]map[int]string
		var GetTable map[int64]Get_Info
		if d.Decode(&Data) != nil ||
			d.Decode(&Processed) != nil || d.Decode(&GetTable) != nil {
			//|| d.Decode(&GetTable) != nil
			fmt.Println("decoding error")
		} else {
			kv.mu.Lock()
			kv.Data = Data
			kv.Processed = Processed
			kv.GetTable = GetTable
			kv.mu.Unlock()
		}

	}

	// You may need initialization code here.

	// Use a goroutine to listen to all the commands that Raft peers have
	// agreed on through applychan. Apply changes to state machine if needed
	go func() {
		for {
			//fmt.Println("infinite loop at background goroutine 1")
			committed := <-kv.applyCh
			if committed.CommandValid {
				temp := committed.Command.(Op)
				//fmt.Println("temp", temp)
				// apply put and append operations to state machine
				//fmt.Println("Applych info", temp.Op_command, temp.Op_key, temp.Op_argument)

				kv.mu.Lock()

				val, exist := kv.Processed[temp.Client_ID]

				//fmt.Println("apply ch command, val, exist, taskid", temp.Op_command, val, exist, temp.Task_ID)
				//kv.mu.Lock()
				if !exist || val < temp.Task_ID {
					// otherwise the task has already been executed

					if temp.Op_command == "Put" {

						kv.Data[temp.Op_key] = temp.Op_argument
						//fmt.Println("Key value after Put", temp.Op_key, kv.Data[temp.Op_key])

					}

					if temp.Op_command == "Append" {

						kv.Data[temp.Op_key] = kv.Data[temp.Op_key] + temp.Op_argument
						//fmt.Println("Key Value after Append", temp.Op_key, kv.Data[temp.Op_key])
					}

					if temp.Op_command == "Get" {
						// GetData
						val, exist := kv.Data[temp.Op_key]

						if exist {
							// update in table
							_, ex := kv.GetTable[temp.Client_ID]
							if !ex {
								// kv.GetTable[temp.Client_ID] = make(map[int]string)
								// kv.GetTable[temp.Client_ID][temp.Task_ID] = val
								get_input := Get_Info{}
								get_input.Latest_Task = temp.Task_ID
								get_input.Reply = val
								kv.GetTable[temp.Client_ID] = get_input

							} else {
								//kv.GetTable[temp.Client_ID][temp.Task_ID] = val
								get_input := Get_Info{}
								get_input.Latest_Task = temp.Task_ID
								get_input.Reply = val
								kv.GetTable[temp.Client_ID] = get_input

							}

						} else {

							_, ex := kv.GetTable[temp.Client_ID]
							if !ex {
								// kv.GetTable[temp.Client_ID] = make(map[int]string)
								// kv.GetTable[temp.Client_ID][temp.Task_ID] = ""
								get_input := Get_Info{}
								get_input.Latest_Task = temp.Task_ID
								get_input.Reply = ""
								kv.GetTable[temp.Client_ID] = get_input

							} else {
								// kv.GetTable[temp.Client_ID][temp.Task_ID] = ""
								get_input := Get_Info{}
								get_input.Latest_Task = temp.Task_ID
								get_input.Reply = ""
								kv.GetTable[temp.Client_ID] = get_input

							}

						}

					}

					// update last processed index for the client
					kv.Processed[temp.Client_ID] = temp.Task_ID

				}

				// send the committed info to the wait channel
				// so that RPC can reply to client
				// Note only the leader needs to reply to the client
				ch, ok := kv.WaitChan[committed.CommandIndex]
				if !ok {
					kv.WaitChan[committed.CommandIndex] = make(chan raft.ApplyMsg, 1)
				} else {
					ch <- committed
				}
				//fmt.Println("server still on", kv.me)
				kv.LogIndex = committed.CommandIndex

				kv.mu.Unlock()

			} else {
				// then we received a snapshot
				if committed.SnapshotValid {

					// then we update state machine with whatever is saved in
					// the snapshot
					r := bytes.NewBuffer(committed.Snapshot)
					d := labgob.NewDecoder(r)
					var Data map[string]string
					var Processed map[int64]int
					//var GetTable map[int64]map[int]string
					var GetTable map[int64]Get_Info
					if d.Decode(&Data) != nil ||
						d.Decode(&Processed) != nil || d.Decode(&GetTable) != nil {
						//|| d.Decode(&GetTable) != nil
						fmt.Println("decoding error")
					} else {
						kv.mu.Lock()
						kv.Data = Data
						kv.Processed = Processed
						kv.GetTable = GetTable
						kv.mu.Unlock()
					}

				} else {
					fmt.Println("GuiGuShi, neither command nor snapshot")
				}

			}

		}

	}()

	// Another goroutine that keeps track of whether MaxRaftState is passed
	// and create snapshots accordingly.
	go func() {
		for {
			kv.mu.Lock()
			current_size := kv.persister.RaftStateSize()
			//fmt.Println("current size", current_size)

			kv.mu.Unlock()
			if maxraftstate == -1 || maxraftstate > current_size {
				//fmt.Println("if, current size, maxraftstate", current_size, maxraftstate)
			} else {
				//fmt.Println("else, Snapshot!", current_size, maxraftstate)

				// else, we need to take snapshot, both the data table and the processed map
				// need to be stored
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				kv.mu.Lock()
				e.Encode(kv.Data)
				e.Encode(kv.Processed)
				e.Encode(kv.GetTable)
				SnapShotData := w.Bytes()
				kv.mu.Unlock()

				kv.rf.Snapshot(kv.LogIndex, SnapShotData)

			}
			time.Sleep(time.Millisecond * 100)

		}

	}()

	return kv
}

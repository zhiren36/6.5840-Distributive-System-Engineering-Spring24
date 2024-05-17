package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs    []Config // indexed by config num
	Processed  map[int64]int
	WaitChan   map[int]chan raft.ApplyMsg
	QueryTable map[int64]Query_Info // stores the response to the latest Query command

}

type Query_Info struct {
	Conig       Config
	Latest_Task int
}

type Op struct {
	// Your data here.
	Op_command        string // join, leave, move or query
	Op_query_argument QueryArgs
	Op_join_argument  JoinArgs
	Op_leave_argument LeaveArgs
	Op_move_argument  MoveArgs // only one of the above arguments are
	// non-nil

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}
	op.Op_command = "Join"
	op.Op_join_argument.Servers = args.Servers
	op.Op_join_argument.Client_ID = args.Client_ID
	op.Op_join_argument.Task_ID = args.Task_ID

	// push to raft
	index, _, isleader := sc.rf.Start(op)

	// if not leader, simply return and do nothing
	if isleader == false {
		reply.WrongLeader = true
		return
	}

	// if wrong group, reply with error

	// wait for raft to reach agreement
	// first, we create a wait channel to receive ApplyMsg's after Raft forms consensus
	sc.mu.Lock()
	_, exist := sc.WaitChan[index]
	if exist {
		fmt.Println("Error, wait channel already exists")
	} else {
		sc.WaitChan[index] = make(chan raft.ApplyMsg, 1)
	}
	sc.mu.Unlock()

	select {

	case Response := <-sc.WaitChan[index]:
		// Raft peers have formed a concensus, we can apply to state machine
		// Need to check if it's the same operation that has already been applied before
		temp := Response.Command.(Op)

		// a sanity check

		if temp.Op_join_argument.Client_ID != args.Client_ID || temp.Op_join_argument.Task_ID != args.Task_ID {
			fmt.Println("GUIGUSHI, Join")
			reply.WrongLeader = true
			return
		}
		reply.Err = OK
		//fmt.Println("Put, Err, key, value, clientID, taskID ", reply.Err, temp.Op_key, "|", temp.Op_argument, args.Client_id, args.Task_id)

	case <-time.After(time.Millisecond * 500):
		//fmt.Println("server timeout when handling Put", kv.me)
		// the client will keep retrying
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	op := Op{}
	op.Op_command = "Leave"
	op.Op_leave_argument.Client_ID = args.Client_ID
	op.Op_leave_argument.Task_ID = args.Task_ID
	op.Op_leave_argument.GIDs = args.GIDs

	// push to raft
	index, _, isleader := sc.rf.Start(op)

	// if not leader, simply return and do nothing
	if isleader == false {
		reply.WrongLeader = true
		return
	}

	// if wrong group, reply with error

	// wait for raft to reach agreement
	// first, we create a wait channel to receive ApplyMsg's after Raft forms consensus
	sc.mu.Lock()
	_, exist := sc.WaitChan[index]
	if exist {
		fmt.Println("Error, wait channel already exists")
	} else {
		sc.WaitChan[index] = make(chan raft.ApplyMsg, 1)
	}
	sc.mu.Unlock()

	select {

	case Response := <-sc.WaitChan[index]:
		// Raft peers have formed a concensus, we can apply to state machine
		// Need to check if it's the same operation that has already been applied before
		temp := Response.Command.(Op)

		// a sanity check

		if temp.Op_leave_argument.Client_ID != args.Client_ID || temp.Op_leave_argument.Task_ID != args.Task_ID {
			fmt.Println("GUIGUSHI, Leave")
			reply.WrongLeader = true
			return
		}
		reply.Err = OK
		//fmt.Println("Put, Err, key, value, clientID, taskID ", reply.Err, temp.Op_key, "|", temp.Op_argument, args.Client_id, args.Task_id)

	case <-time.After(time.Millisecond * 500):
		//fmt.Println("server timeout when handling Put", kv.me)
		// the client will keep retrying
		reply.WrongLeader = true
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	op := Op{}
	op.Op_command = "Move"
	op.Op_move_argument.Client_ID = args.Client_ID
	op.Op_move_argument.Task_ID = args.Task_ID
	op.Op_move_argument.GID = args.GID
	op.Op_move_argument.Shard = args.Shard

	// move command does not require a reply, so we handle everything
	// in the background goroutine

	// push to raft
	index, _, isleader := sc.rf.Start(op)

	// if not leader, simply return and do nothing
	if isleader == false {
		reply.WrongLeader = true
		return
	}

	// if wrong group, reply with error

	// wait for raft to reach agreement
	// first, we create a wait channel to receive ApplyMsg's after Raft forms consensus
	sc.mu.Lock()
	_, exist := sc.WaitChan[index]
	if exist {
		fmt.Println("Error, wait channel already exists")
	} else {
		sc.WaitChan[index] = make(chan raft.ApplyMsg, 1)
	}
	sc.mu.Unlock()

	select {

	case Response := <-sc.WaitChan[index]:
		// Raft peers have formed a concensus, we can apply to state machine
		// Need to check if it's the same operation that has already been applied before
		temp := Response.Command.(Op)

		// a sanity check

		if temp.Op_move_argument.Client_ID != args.Client_ID || temp.Op_move_argument.Task_ID != args.Task_ID {
			fmt.Println("GUIGUSHI, Move")
			reply.WrongLeader = true
			return
		}
		reply.Err = OK
		//fmt.Println("Put, Err, key, value, clientID, taskID ", reply.Err, temp.Op_key, "|", temp.Op_argument, args.Client_id, args.Task_id)

	case <-time.After(time.Millisecond * 500):
		//fmt.Println("server timeout when handling Put", kv.me)
		// the client will keep retrying
		reply.WrongLeader = true
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	op := Op{}
	op.Op_command = "Query"
	op.Op_query_argument.Client_ID = args.Client_ID
	op.Op_query_argument.Task_ID = args.Task_ID
	op.Op_query_argument.Num = args.Num

	sc.mu.Lock()
	rep, ok := sc.QueryTable[args.Client_ID]
	sc.mu.Unlock()
	if ok && args.Task_ID == rep.Latest_Task {
		reply.Config = rep.Conig
		reply.Err = OK
		return
	}
	if ok && args.Task_ID < rep.Latest_Task {
		fmt.Println("GuiGuShi, Querry args.Task_id < rep.Latest_Task")
	}

	// push to raft
	//kv.mu.Unlock()
	index, _, isleader := sc.rf.Start(op)
	//kv.mu.Lock()

	// if not leader, simply return and do nothing
	if isleader == false {
		reply.WrongLeader = true
		return
	}

	// wait for raft to reach agreement
	// first, we create a wait channel to receive ApplyMsg's after Raft forms consensus
	sc.mu.Lock()
	_, exist := sc.WaitChan[index]
	if exist {
		fmt.Println("Error, wait channel already exists")
	} else {
		sc.WaitChan[index] = make(chan raft.ApplyMsg, 1)
	}

	sc.mu.Unlock()

	select {

	case Response := <-sc.WaitChan[index]:
		// Raft peers have formed a concensus, we can apply to state machine
		// Need to check if it's the same operation that has already been applied before
		temp := Response.Command.(Op)
		// just a sanity check

		if temp.Op_query_argument.Client_ID != op.Op_query_argument.Client_ID || temp.Op_query_argument.Task_ID != op.Op_query_argument.Task_ID || temp.Op_command != "Query" {
			fmt.Println("GUIGUSHI, Query")
			reply.WrongLeader = true
			return
		}
		sc.mu.Lock()
		a, e := sc.QueryTable[temp.Op_query_argument.Client_ID]
		if e {
			reply.Err = OK
			reply.Config = a.Conig

		} else {
			fmt.Println("GuiGuShi Entry Disappeared in QueryTable")
			reply.WrongLeader = true
		}

		sc.mu.Unlock()
		//fmt.Println("Get, Err, key, value, clien_id, taskID ", reply.Err, args.Key, " | ", reply.Value, args.Client_id, args.Task_id)

	case <-time.After(time.Millisecond * 500):
		//fmt.Println("server timeout when handling Get", kv.me)
		// the client will keep retrying
		reply.WrongLeader = true

	}

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.Processed = make(map[int64]int)
	sc.QueryTable = make(map[int64]Query_Info)
	sc.WaitChan = make(map[int]chan raft.ApplyMsg)

	// a background go routine that listens to all the commands

	// note that we do not deal with snapshots yet

	go func() {

		for {
			committed := <-sc.applyCh
			temp := committed.Command.(Op)
			sc.mu.Lock()
			command := temp.Op_command
			if command == "Query" {

				val, exist := sc.Processed[temp.Op_query_argument.Client_ID]

				if !exist || val < temp.Op_query_argument.Task_ID {
					// otherwise the task has already been executed

					// first fetch the query information

					var q Config
					if temp.Op_query_argument.Num == -1 {
						q = sc.configs[len(sc.configs)-1]

					} else {
						if temp.Op_query_argument.Num <= len(sc.configs)-1 {
							q = sc.configs[temp.Op_query_argument.Num]
						} else {
							q = Config{}
						}
					}
					reply := Query_Info{}
					reply.Conig = q
					reply.Latest_Task = temp.Op_query_argument.Task_ID

					sc.QueryTable[temp.Op_query_argument.Client_ID] = reply

					// update last processed index for the client
					sc.Processed[temp.Op_query_argument.Client_ID] = temp.Op_query_argument.Task_ID

				}

			}

			if command == "Move" {

				val, exist := sc.Processed[temp.Op_move_argument.Client_ID]

				if !exist || val < temp.Op_move_argument.Task_ID {
					// then we need to perform the move

					// First create a new configuration
					new_config := Config{}
					new_config.Groups = make(map[int][]string)
					new_config.Shards = [NShards]int{}

					new_config.Num = sc.configs[len(sc.configs)-1].Num + 1
					// copy shards

					for i := 0; i < NShards; i++ {
						new_config.Shards[i] = sc.configs[len(sc.configs)-1].Shards[i]
					}
					// copy groups
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						new_config.Groups[k] = v
					}
					// assigning the shard to a different group
					new_config.Shards[temp.Op_move_argument.Shard] = temp.Op_move_argument.GID
					sc.configs = append(sc.configs, new_config)

					sc.Processed[temp.Op_move_argument.Client_ID] = temp.Op_move_argument.Task_ID

				}

			}

			if command == "Join" {

				val, exist := sc.Processed[temp.Op_join_argument.Client_ID]
				if !exist || val < temp.Op_join_argument.Task_ID {

					// first, add these new groups to the map from GIDs to server names

					// new_config := sc.configs[len(sc.configs)-1]
					// new_config.Num = new_config.Num + 1
					new_config := Config{}
					new_config.Groups = make(map[int][]string)
					new_config.Shards = [NShards]int{}
					new_config.Num = sc.configs[len(sc.configs)-1].Num + 1
					// copy groups
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						new_config.Groups[k] = v
					}
					// add new groups
					for k, v := range temp.Op_join_argument.Servers {
						new_config.Groups[k] = v
					}
					// copy shards
					for k, v := range sc.configs[len(sc.configs)-1].Shards {
						new_config.Shards[k] = v
					}

					// Then we need to rebalance the shards

					if len(new_config.Groups) == 0 {

						new_config.Shards = [NShards]int{}
					} else if len(new_config.Groups) == 1 {
						// assign all the shards to this group
						for gid, _ := range new_config.Groups {
							for i, _ := range new_config.Shards {
								new_config.Shards[i] = gid
							}
						}
					} else {

						avg := NShards / len(new_config.Groups)
						remainder := NShards - avg*len(new_config.Groups)
						var GIDs []int
						for i, _ := range new_config.Groups {
							GIDs = append(GIDs, i)
						}
						sort.Ints(GIDs)
						var Counts []int

						// loop over all the GIDs to check
						// whether a group has more or less shards than avg
						count3 := 0

						for i := 0; i < len(GIDs); i++ {
							gid := GIDs[i]
							count := 0

							for _, j := range new_config.Shards {
								if gid == j {
									count++

								}
							}
							Counts = append(Counts, count)

							// the first raminder groups with shard >= avg+1,
							// we make shards after avg+1 unallocated

							if count > avg {

								count2 := 0
								for j := 0; j < NShards; j++ {
									if new_config.Shards[j] == gid {
										count2++

										if count3 <= remainder-1 {
											if count2 > avg+1 {
												new_config.Shards[j] = 0
											}
										} else {
											if count2 > avg {
												new_config.Shards[j] = 0
											}

										}

									}

								}
								count3++
							}

						}
						//fmt.Println("after loop 1", new_config.Shards)

						// after the above step, all the groups have shards <= avg
						// we assign the unallocated shards to those < avg
						for i := 0; i < len(new_config.Groups); i++ {
							if Counts[i] < avg {
								// assign unallocated shard until it reaches avg
								for j := 0; j < NShards; j++ {

									if Counts[i] == avg {
										break
									}

									if new_config.Shards[j] == 0 {
										new_config.Shards[j] = GIDs[i]
										Counts[i]++
									}
								}

							}
						}
						//fmt.Println("after loop2", new_config.Shards)
						// after the above step, all the groups shoud have
						// shards equal to avg

						// assign the remainder number of shards to the smallest
						// remainder number of groups
						ind := 0
						for i := 0; i < NShards; i++ {
							if new_config.Shards[i] == 0 {
								new_config.Shards[i] = GIDs[ind]
								ind++

							}
						}

					}
					sc.configs = append(sc.configs, new_config)

					// update the processing table
					sc.Processed[temp.Op_join_argument.Client_ID] = temp.Op_join_argument.Task_ID

				}

			}

			if command == "Leave" {
				val, exist := sc.Processed[temp.Op_leave_argument.Client_ID]

				if !exist || val < temp.Op_leave_argument.Task_ID {

					new_config := Config{}
					new_config.Groups = make(map[int][]string)
					new_config.Shards = [NShards]int{}
					new_config.Num = sc.configs[len(sc.configs)-1].Num + 1
					// copy groups
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						new_config.Groups[k] = v
					}
					// copy shards
					for k, v := range sc.configs[len(sc.configs)-1].Shards {
						new_config.Shards[k] = v
					}

					// delete groups
					for _, i := range temp.Op_leave_argument.GIDs {
						delete(new_config.Groups, i)
					}
					// assign shards of deleted groups to -1
					for i := 0; i < NShards; i++ {
						for _, j := range temp.Op_leave_argument.GIDs {
							if new_config.Shards[i] == j {
								new_config.Shards[i] = 0
							}
						}
					}

					// rebalance the shards

					if len(new_config.Groups) == 0 {

						new_config.Shards = [NShards]int{}
					} else if len(new_config.Groups) == 1 {
						// assign all the shards to this group
						for gid, _ := range new_config.Groups {
							for i, _ := range new_config.Shards {
								new_config.Shards[i] = gid
							}
						}
					} else {

						avg := NShards / len(new_config.Groups)
						remainder := NShards - avg*len(new_config.Groups)
						var GIDs []int
						for i, _ := range new_config.Groups {
							GIDs = append(GIDs, i)
						}
						sort.Ints(GIDs)
						var Counts []int
						// loop over all the GIDs to check
						// whether a group has more or less shards than avg

						count3 := 0

						for i := 0; i < len(GIDs); i++ {
							gid := GIDs[i]
							count := 0

							for _, j := range new_config.Shards {
								if gid == j {
									count++

								}
							}
							Counts = append(Counts, count)

							if count > avg {

								count2 := 0
								for j := 0; j < NShards; j++ {
									if new_config.Shards[j] == gid {
										count2++

										if count3 <= remainder-1 {
											if count2 > avg+1 {
												new_config.Shards[j] = 0
											}
										} else {
											if count2 > avg {
												new_config.Shards[j] = 0
											}

										}

									}

								}
								count3++
							}

						}
						// after the above step, all the groups have shards <= avg
						// we assign the unallocated shards to those < avg
						for i := 0; i < len(new_config.Groups); i++ {
							if Counts[i] < avg {
								// assign unallocated shard until it reaches avg
								for j := 0; j < NShards; j++ {

									if Counts[i] == avg {
										break
									}

									if new_config.Shards[j] == 0 {
										new_config.Shards[j] = GIDs[i]
										Counts[i]++
									}
								}

							}
						}
						// after the above step, all the groups shoud have
						// shards equal to avg

						// assign the remainder number of shards to the smallest
						// remainder number of groups
						ind := 0
						for i := 0; i < NShards; i++ {
							if new_config.Shards[i] == 0 {
								new_config.Shards[i] = GIDs[ind]
								ind++

							}
						}

					}
					sc.configs = append(sc.configs, new_config)

					// update the processing table
					sc.Processed[temp.Op_leave_argument.Client_ID] = temp.Op_leave_argument.Task_ID

				}

			}

			// send the committed info to the wait channel
			// so that RPC can reply to client
			// Note only the leader needs to reply to the client
			ch, ok := sc.WaitChan[committed.CommandIndex]
			if !ok {
				sc.WaitChan[committed.CommandIndex] = make(chan raft.ApplyMsg, 1)
			} else {
				ch <- committed
			}

			sc.mu.Unlock()
		}

	}()

	return sc
}

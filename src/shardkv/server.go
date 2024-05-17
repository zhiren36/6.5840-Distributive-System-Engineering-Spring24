package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CmdNum    int    // command number
	OpType    string // indicate whether the op is for client or configuration
	Key       string
	Value     string
	NewConfig shardctrler.Config // new config, only for update configuration command
	CfgNum    int                // configuration number（only for GetShard and GiveShard）
	ShardNum  int                // shard number（only for GetShard and GiveShard）
	ShardData map[string]string  // only for GetShard (to transmit transferred data)
	Processed map[int64]Request  // only for GetShard (to transmit transffered data)
}

// this is for filtering our repeated requests
type Request struct {
	LastCmdNum int // last command number processed for a client
	OpType     string
	Response   Reply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int

	// Your definitions here.
	dead            int32 // set by Kill()
	mck             *shardctrler.Clerk
	kvDB            map[int]map[string]string       // database, organized by shard
	processed_table map[int64]Request               // for filtering out repeated requests
	notifyMapCh     map[int]chan Reply              // for letting server know when a command gets applied
	logLastApplied  int                             // last applied index in raft's log
	shardstates     [shardctrler.NShards]ShardState // state of shards, be be exist, noexsit,
	preConfig       shardctrler.Config
	curConfig       shardctrler.Config
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid // servers in a group won't change
	kv.masters = masters

	// Your initialization code here.
	kv.dead = 0
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardctrler.MakeClerk(kv.masters)

	kv.kvDB = make(map[int]map[string]string)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.kvDB[i] = make(map[string]string)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.processed_table = make(map[int64]Request)
	kv.notifyMapCh = make(map[int]chan Reply)
	kv.logLastApplied = 0

	kv.preConfig = shardctrler.Config{}
	kv.curConfig = shardctrler.Config{}
	var shardsArr [shardctrler.NShards]ShardState
	kv.shardstates = shardsArr
	// start a goroutine that listens to the apply channel
	// and apply all the committed commands accordingly
	go kv.applyMessage()
	// a goroutine that keeps updating configurations
	go func() {
		for {
			// check whether this server is the leader
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				time.Sleep(time.Millisecond * 60)
				continue
			}

			// we only update configuration if shard migration is done
			// that is, if state is either exist or noexist
			updateable := true
			kv.mu.Lock()

			for _, state := range kv.shardstates {
				if state != NoExist && state != Exist {
					updateable = false
				}
			}

			if !updateable {
				time.Sleep(time.Millisecond * 60)
				kv.mu.Unlock()
				continue
			}

			curConfig := kv.curConfig
			kv.mu.Unlock()

			//nextConfig := kv.mck.Query(curConfig.Num + 1)

			LargestNum := kv.mck.Query(-1).Num

			// if next configuration exists, it means we need to update

			if LargestNum > curConfig.Num {
				nextConfig := kv.mck.Query(curConfig.Num + 1)
				configUpdateOp := Op{
					OpType:    UpdateConfig,
					NewConfig: nextConfig,
					CfgNum:    nextConfig.Num,
				}
				// push to raft so that followers can update as well
				kv.rf.Start(configUpdateOp)
			}

			time.Sleep(time.Millisecond * 60)
		}

	}()

	// a goroutine the keeps track of whether this group needs to get shard
	// from another group. Start migration process if it does.

	go func() {
		for {
			// check isleader
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				time.Sleep(time.Millisecond * 60)
				continue
			}

			var PullingShards []int
			kv.mu.Lock()
			for shard, state := range kv.shardstates {
				if state == Pulling {
					PullingShards = append(PullingShards, shard)
				}
			}
			preConfig := kv.preConfig
			curConfigNum := kv.curConfig.Num
			kv.mu.Unlock()

			// Use WaitGroup to make sure that every needed shard is asked in a round
			var wg sync.WaitGroup

			for _, shard := range PullingShards {
				wg.Add(1)
				preGid := preConfig.Shards[shard]      // get gis of previous owner
				preServers := preConfig.Groups[preGid] // get server's named based on gid
				go func(servers []string, configNum int, shardNum int) {
					defer wg.Done()
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						args := MigrateArgs{
							ConfigNum: configNum,
							ShardNum:  shardNum,
						}
						reply := MigrateReply{}
						ok := srv.Call("ShardKV.MigrateShard", &args, &reply)

						if !ok || (ok && reply.Err == ErrWrongLeader) {
							continue
						}

						if ok && reply.Err == ErrNotReady {
							// if the group is still being updated, we just wait until next round of asking
							break
						}

						if ok && reply.Err == OK {
							// we succesfully obtain shard data, push it into raft
							// so that everyone in the group gets the same data.
							getShardOp := Op{
								OpType:    GetShard,
								CfgNum:    configNum,
								ShardNum:  shardNum,
								ShardData: reply.ShardData,
								Processed: reply.RequestData,
							}
							kv.rf.Start(getShardOp)
							break
						}

					}
				}(preServers, curConfigNum, shard)
			}
			wg.Wait() // block until all the RPCs are done

			time.Sleep(time.Millisecond * 60)
		}

	}()

	// a goroutine that keeps track of whether a shard is migrated away
	// we need to delete all the data to release memeory
	go func() {
		for {
			// checks whether it is leader
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				time.Sleep(time.Millisecond * 60)
				continue
			}

			var SendingShards []int
			kv.mu.Lock()
			for shard, state := range kv.shardstates {
				if state == Sending {
					SendingShards = append(SendingShards, shard)
				}
			}
			// current configuration to obtain owners of these shards
			curConfig := kv.curConfig
			kv.mu.Unlock()

			// everything below is similar to the migration process
			// except we now just ask them whether they received data
			var wg sync.WaitGroup

			for _, shard := range SendingShards {
				wg.Add(1)
				Gid := curConfig.Shards[shard]
				Servers := curConfig.Groups[Gid]

				go func(servers []string, configNum int, shardNum int) {
					defer wg.Done()
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						args := AckArgs{
							ConfigNum: configNum,
							ShardNum:  shardNum,
						}
						reply := AckReply{}
						ok := srv.Call("ShardKV.AckReceiveShard", &args, &reply)

						if !ok || (ok && reply.Err == ErrWrongLeader) {
							continue
						}

						if ok && reply.Err == ErrNotReady {
							break
						}

						if ok && reply.Err == OK && !reply.Receive {
							break
						}

						if ok && reply.Err == OK && reply.Receive {
							// shard migration is done, we push an Op
							// to raft so that everyone in the group
							// can update their information
							giveShardOp := Op{
								OpType:   GiveShard,
								CfgNum:   configNum,
								ShardNum: shardNum,
							}
							kv.rf.Start(giveShardOp)
							break
						}

					}
				}(Servers, curConfig.Num, shard)
			}
			wg.Wait() // blocks until all RPCs are done

			time.Sleep(time.Millisecond * 60)
		}
	}()

	return kv
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.shardstates[shard] != Exist {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	getOp := Op{}
	getOp.ClientId = args.ClientId
	getOp.CfgNum = args.CmdNum
	getOp.OpType = Get
	getOp.Key = args.Key

	// push to raft and check whether it is leader

	index, _, isLeader := kv.rf.Start(getOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := make(chan Reply, 1)
	kv.notifyMapCh[index] = notifyCh
	kv.mu.Unlock()

	// wait until timeout or the command gets sent to applychannel
	select {
	case res := <-notifyCh:
		// recheck if the group is still reponsible for the key（in case config change during wait process）
		kv.mu.Lock()
		if kv.shardstates[shard] != Exist {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.notifyMapCh, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	shard := key2shard(args.Key)
	if kv.shardstates[shard] != Exist {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if args.CmdNum < kv.processed_table[args.ClientId].LastCmdNum {

		kv.mu.Unlock()
		return
	} else if args.CmdNum == kv.processed_table[args.ClientId].LastCmdNum {

		reply.Err = kv.processed_table[args.ClientId].Response.Err
		kv.mu.Unlock()
		return
	} else {
		// else, this is a new request
		kv.mu.Unlock() // Need to Unlock here!
		paOp := Op{}
		paOp.ClientId = args.ClientId
		paOp.CmdNum = args.CmdNum
		paOp.OpType = args.Op
		paOp.Key = args.Key
		paOp.Value = args.Value

		index, _, isLeader := kv.rf.Start(paOp)

		// check if it's leader
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.Lock()
		notifyCh := make(chan Reply, 1)
		kv.notifyMapCh[index] = notifyCh
		kv.mu.Unlock()

		// wait until command gets sent to apply channel by raft
		select {
		case res := <-notifyCh:
			// check if the shard is still on this group
			kv.mu.Lock()
			if kv.shardstates[shard] != Exist {
				reply.Err = ErrWrongGroup
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			reply.Err = res.Err

		case <-time.After(500 * time.Millisecond):
			reply.Err = ErrTimeout
		}

		kv.mu.Lock()
		delete(kv.notifyMapCh, index)
		kv.mu.Unlock()
	}
}

// applying all the commands in raft's applychannel
func (kv *ShardKV) applyMessage() {
	for {
		applyMsg := <-kv.applyCh // reading applied messages from raft

		if applyMsg.CommandValid {
			// applyMsg is for command
			op, _ := applyMsg.Command.(Op)

			if op.OpType == Get || op.OpType == Put || op.OpType == Append {
				kv.ClientOps(op, applyMsg.CommandIndex, applyMsg.CommandTerm)
			}
			if op.OpType == UpdateConfig || op.OpType == GetShard || op.OpType == GiveShard {
				kv.ConfigOps(op, applyMsg.CommandIndex)
			}

		} else if applyMsg.SnapshotValid {

			// this is yet to be implemented
		}
	}
}

// execute clientops
func (kv *ShardKV) ClientOps(op Op, commandIndex int, commandTerm int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(op.Key)
	if kv.shardstates[shard] != Exist || commandIndex <= kv.logLastApplied {
		return
	}

	// set loglastapplied
	kv.logLastApplied = commandIndex

	reply := Reply{}
	RequestRec, exist := kv.processed_table[op.ClientId]

	// filter out repeateed requests
	if exist && op.OpType != Get && op.CmdNum <= RequestRec.LastCmdNum {
		// get cammand can be executed multiple times
		reply = kv.processed_table[op.ClientId].Response
		// reply with whatever is on record
	} else {

		// else, this is a new command

		if op.OpType == Get {
			v, ok := kv.kvDB[shard][op.Key]
			if !ok {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Err = OK
				reply.Value = v
			}
		}
		if op.OpType == Put {
			kv.kvDB[shard][op.Key] = op.Value
			reply.Err = OK
		}

		if op.OpType == Append {
			oldValue, existKey := kv.kvDB[shard][op.Key]
			if !existKey {
				reply.Err = ErrNoKey
				kv.kvDB[shard][op.Key] = op.Value
			} else {
				reply.Err = OK
				kv.kvDB[shard][op.Key] = oldValue + op.Value
			}
		}

		// add to the processed table so that future requests with the same
		// id will be filtered out
		if op.OpType != Get {
			Request := Request{
				LastCmdNum: op.CmdNum,
				OpType:     op.OpType,
				Response:   reply,
			}
			kv.processed_table[op.ClientId] = Request
		}
	}

	// if still the leader, send the info the notifychannel so that the RPC can
	// reply to client
	_, ok := kv.notifyMapCh[commandIndex]
	if ok {
		currentTerm, isLeader := kv.rf.GetState()
		if isLeader && commandTerm == currentTerm {
			kv.notifyMapCh[commandIndex] <- reply
		}
	}
}

// RPC handller for asking a shard from another group
func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	// check for leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if kv's cofignumber is less than args.ConfigNum, it means we are not up to date yet
	// let asker wait
	if kv.curConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// this happens rarely, but may happen if an old request takes a long time to arrive
	if kv.curConfig.Num > args.ConfigNum {
		return
	}

	// only when the config numbers are equal, we send shard data
	kvMap := map[string]string{}
	for k, v := range kv.kvDB[args.ShardNum] {
		kvMap[k] = v
	}
	reply.ShardData = kvMap

	// need to send out request as well, for the shard owner to filter out repeated requests
	processed_table := map[int64]Request{}
	for k, v := range kv.processed_table {
		processed_table[k] = v
	}
	reply.RequestData = processed_table
	reply.Err = OK
}

// excuting operations about configurations in the applier
func (kv *ShardKV) ConfigOps(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commandIndex <= kv.logLastApplied {
		return
	}

	kv.logLastApplied = commandIndex

	// executing shard commands

	if op.OpType == UpdateConfig {
		if kv.curConfig.Num+1 == op.CfgNum {
			//kv.updateShardState(op.NewConfig)
			for shard, gid := range op.NewConfig.Shards {

				if kv.shardstates[shard] == Exist && gid != kv.gid {
					kv.shardstates[shard] = Sending
				}
				if kv.shardstates[shard] == NoExist && gid == kv.gid {
					if op.CfgNum == 1 {
						kv.shardstates[shard] = Exist
					} else {
						kv.shardstates[shard] = Pulling
					}
				}

			}

			if kv.preConfig.Num != 0 || kv.curConfig.Num != 0 {
				kv.preConfig = kv.curConfig
			}
			kv.curConfig = op.NewConfig
		}

	}
	if op.OpType == GetShard {
		// apply shard obtain from another group to kvDB
		if kv.curConfig.Num == op.CfgNum && kv.shardstates[op.ShardNum] == Pulling {
			kvMap := map[string]string{}
			for k, v := range op.ShardData {
				kvMap[k] = v
			}
			kv.kvDB[op.ShardNum] = kvMap
			kv.shardstates[op.ShardNum] = Exist
			for clientId, Request := range op.Processed {
				if ownRequest, exist := kv.processed_table[clientId]; !exist || Request.LastCmdNum > ownRequest.LastCmdNum {
					kv.processed_table[clientId] = Request
				}
			}

		}
	}
	if op.OpType == GiveShard {
		// the shard has been given away, so we need to delete it
		if kv.curConfig.Num == op.CfgNum && kv.shardstates[op.ShardNum] == Sending {
			kv.shardstates[op.ShardNum] = NoExist
			kv.kvDB[op.ShardNum] = map[string]string{}

		}

	}

}

// check that all the Sending shards have been received succesfully by the other group
// so we can update it to Noexist
func (kv *ShardKV) AckReceiveShard(args *AckArgs, reply *AckReply) {
	// check for leadership
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if kv.curConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	if kv.curConfig.Num > args.ConfigNum || kv.shardstates[args.ShardNum] == Exist {
		reply.Receive = true
		reply.Err = OK
		return
	}

	reply.Receive = false
	reply.Err = OK
}

package kvraft

import (
	crand "crypto/rand"
	"math/big"
	"math/rand"
	"sync"

	"6.5840/labrpc"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	Client_id  int64
	Task_id    int
	LastLeader int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.Client_id = nrand()
	ck.Task_id = 0
	// Initialize a random server as last leader
	ck.LastLeader = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{}
	args.Key = key
	args.Client_id = ck.Client_id

	ck.mu.Lock()
	ck.Task_id = ck.Task_id + 1
	args.Task_id = ck.Task_id
	ck.mu.Unlock()

	for {

		// randomly select a server, if it is not the leader (ErrWrongLeader is returned)
		// then go to another server
		reply := GetReply{}
		ok := ck.servers[ck.LastLeader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			return reply.Value
		} else {
			// try another server to see if it is the leader
			ck.mu.Lock()
			ck.LastLeader = rand.Intn(len(ck.servers))
			ck.mu.Unlock()
			continue
		}

	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Client_id = ck.Client_id

	ck.mu.Lock()
	ck.Task_id = ck.Task_id + 1

	args.Task_id = ck.Task_id
	ck.mu.Unlock()

	for {

		// randomly select a server, if it is not the leader (ErrWrongLeader is returned)
		// then go to another server
		reply := PutAppendReply{}
		ok := ck.servers[ck.LastLeader].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			return
		} else {
			// try another server to see if it is the leader
			ck.mu.Lock()
			ck.LastLeader = rand.Intn(len(ck.servers))
			ck.mu.Unlock()
			continue
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

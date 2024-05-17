package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.

	// A map that stores all Key/Value pairs
	Data map[string]string

	// A map that stores task IDs and response
	Task_Response map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.Data[args.Key]
	if ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Data[args.Key] = args.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// First we check if the task is already done. If it is already done,
	// we just reply with the response stored in our table

	response, exist := kv.Task_Response[args.Task_id]

	if exist {
		reply.Value = response
	} else {

		old, found := kv.Data[args.Key]

		if found {

			kv.Data[args.Key] = kv.Data[args.Key] + args.Value
			reply.Value = old
			kv.Task_Response[args.Task_id] = reply.Value

		} else {

			kv.Data[args.Key] = args.Value
			reply.Value = ""
			kv.Task_Response[args.Task_id] = reply.Value
		}

	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.Data = make(map[string]string)

	kv.Task_Response = make(map[int64]string)

	return kv
}

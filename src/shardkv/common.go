package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const NShards = 10

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CmdNum   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	CmdNum   int
}

type GetReply struct {
	Err   Err
	Value string
}

type Reply struct {
	Err   Err
	Value string
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
)

// OpType
const (
	Get          = "Get"
	Put          = "Put"
	Append       = "Append"
	UpdateConfig = "UpdateConfig" //
	GetShard     = "GetShard"     // groups already has shard data，need to update to kbDB and	WaitGet -> Exist
	GiveShard    = "GiveShard"    // group has given away the shard，need to update WaitGive -> NoExist
)

type ShardState int

// shard types
const (
	NoExist ShardState = iota // shard not belong to the group
	Exist                     // shard belongs to group and can serve normally
	Pulling                   // shard belongs to the group but waiting for migration
	Sending                   // shard not belong to group but waitin for migration to be done ）
)

type MigrateArgs struct {
	ConfigNum int
	ShardNum  int
}

type MigrateReply struct {
	ShardData   map[string]string
	RequestData map[int64]Request
	Err         Err
}

type AckArgs struct {
	ConfigNum int
	ShardNum  int
}

type AckReply struct {
	Receive bool
	Err     Err
}

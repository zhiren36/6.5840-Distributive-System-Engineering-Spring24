package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Note some of these states will be moved to persister to store on hard disk
	// This will be implemented in labe 3C.

	currentTerm int        // Current term number the server is in
	serverState int        // 0 for follower, 1 for candidate and 2 for leader
	log         []logEntry // Stores log information.
	votedFor    int        // The server number in peers that this raft server voted for.
	lastRPC     time.Time  // Stores the timestamp of last RPC call received.
	// Used by followers to determine if a leader is dead.
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders:
	nextIndex  []int // For each server, index of the next log entry to send to that server
	matchIndex []int // For each server, index of highest log entry known to be replicated
	// on server
	applyCh chan ApplyMsg // For sending commited commands to be executed

	snapshotData      []byte // Stores snapshot data
	lastIncludedIndex int    // last included index in snapshot
	lastIncludedTerm  int    // last included term in snapshot

}

// An entry in a Raft server's log
type logEntry struct {
	Term    int // Leader's term when the command is received
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm
	if rf.serverState == 2 {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}
func (rf *Raft) CheckCurrentTermLog() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var LastLogTerm int
	if len(rf.log) == 0 {
		LastLogTerm = rf.lastIncludedTerm
	} else {
		LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if rf.currentTerm == LastLogTerm {
		return true
	}

	return false
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	if rf.lastIncludedIndex == 0 {
		// then no snapshots have been created
		rf.persister.Save(raftstate, nil)
	} else {
		rf.persister.Save(raftstate, rf.snapshotData)

	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var vote int
	var lg []logEntry
	var commitIndex int
	var lastapplied int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&term) != nil ||
		d.Decode(&vote) != nil ||
		d.Decode(&lg) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastapplied) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("decoding error")
	} else {
		rf.currentTerm = term
		rf.votedFor = vote
		rf.log = lg
		rf.commitIndex = commitIndex
		rf.lastApplied = lastapplied
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

func (rf *Raft) Get_Persist_Size() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Term              int    // Leader's term
	LeaderID          int    // leader's id
	SnapshotData      []byte // a copy of a snapshot
	LastIncludedIndex int
	LastIncludedTerm  int
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		// snaphot index is outdated
		return
	}
	// Your code here (3D).
	// Stores SnapshotData and persist
	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex-1].Term
	rf.snapshotData = snapshot
	// Trim the log up to and including index (which is located at index-rf.lastIncludedIndex -1)
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index

	// Persist
	rf.persist()
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastRPC = time.Now()

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		return
	} else {

		rf.serverState = 0
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.persist()
		}
		rf.currentTerm = args.Term
		rf.persist()

		if args.LastIncludedIndex >= rf.lastIncludedIndex+len(rf.log) {
			// the snapshot sent by leader covers more than our entire log
			// we discord the entire log and apply Snapshot to state machine
			rf.log = []logEntry{}
			// Reset state machine
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = args.LastIncludedIndex
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.snapshotData = args.SnapshotData
			rf.persist()
			apply := ApplyMsg{}
			apply.SnapshotValid = true
			apply.Snapshot = args.SnapshotData
			apply.SnapshotIndex = args.LastIncludedIndex
			apply.SnapshotTerm = args.LastIncludedTerm
			rf.mu.Unlock()
			rf.applyCh <- apply
			rf.mu.Lock()
			reply.Term = rf.currentTerm
			reply.Success = true
			//fmt.Println("apply through snapshot, rf.me, rf.lastApplied, rf.commitIndex", rf.me, rf.lastApplied, rf.commitIndex)

		} else {

			if args.LastIncludedIndex < rf.lastIncludedIndex {
				fmt.Println("GuiGuShi in InstallSnapshot, args.LastIncludedIndex < rf.lastIncludedIndex")
				return
			}

			// else, snapshot only covers part of our log, we need to truncate the
			// log and keep the tail part

			rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
			// Reset state machine
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = args.LastIncludedIndex
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.snapshotData = args.SnapshotData
			rf.persist()
			apply := ApplyMsg{}
			apply.SnapshotValid = true
			apply.Snapshot = args.SnapshotData
			apply.SnapshotIndex = args.LastIncludedIndex
			apply.SnapshotTerm = args.LastIncludedTerm
			rf.mu.Unlock()
			rf.applyCh <- apply
			rf.mu.Lock()
			reply.Term = rf.currentTerm
			reply.Success = true

		}

	}

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int       // Candidate's term
	CandidateId  int       // ID of candidate requesting vote
	Timestamp    time.Time // the timestamp of the request
	LastLogIndex int       // to compare if the leader's log is more uptodate
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	Term        int  // currentTerm on follower's record, for candiate to update itself.
	Votegranted bool // true if the candidate succesfully received the vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reply false if term <= currentTerm
	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.Votegranted = false
	} else {

		lastIndex := rf.lastIncludedIndex + len(rf.log)
		var lastTerm int
		if len(rf.log) >= 1 {
			lastTerm = rf.log[len(rf.log)-1].Term
		} else {
			lastTerm = rf.lastIncludedTerm
		}

		if args.Term > rf.currentTerm {
			// then we can vote
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.serverState = 0
			rf.persist()

			if (args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm &&
				args.LastLogIndex >= lastIndex)) && rf.votedFor == -1 { //new change
				rf.votedFor = args.CandidateId
				rf.persist()
				reply.Term = rf.currentTerm
				reply.Votegranted = true
				rf.lastRPC = args.Timestamp

			} else {
				reply.Votegranted = false
				reply.Term = rf.currentTerm

			}

		} else {

			// Check if the server has already voted in this term
			if rf.votedFor == -1 {
				// changes being made! Be carefull!!!!!!!!!
				// rf.currentTerm changed to lastTerm
				if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm &&
					args.LastLogIndex >= lastIndex) {

					reply.Term = rf.currentTerm
					reply.Votegranted = true
					rf.lastRPC = args.Timestamp
					rf.votedFor = args.CandidateId
					rf.persist()

				} else {
					reply.Term = rf.currentTerm
					reply.Votegranted = false
				}

			} else {
				reply.Term = rf.currentTerm
				reply.Votegranted = false
			}

		}

	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	var index int
	var term int
	var isleader bool

	// Your code here (3B).
	// After we receive command from the client, we store it in the log
	rf.mu.Lock()
	index = len(rf.log) + rf.lastIncludedIndex + 1
	term = rf.currentTerm
	if rf.serverState == 2 {
		isleader = true
	} else {
		isleader = false
	}

	if isleader {
		//fmt.Println("leader, start", rf.me, index, command)
		input := logEntry{}
		input.Command = command
		input.Term = rf.currentTerm
		rf.log = append(rf.log, input)
		//fmt.Println("raft start, me, index, command", rf.me, index, command)
		rf.persist()
		temp := rf.currentTerm
		rf.mu.Unlock()

		// to make raft run fast enough, we need to send out a heartbeat immediately
		// once a new command is pushed to log
		for ind, _ := range rf.peers {

			if ind != rf.me {

				// then send out corresponding RPC calls to each server concurrently
				go func(ind int) {
					rf.mu.Lock()

					// first, we need to decide if we send AppenEntries or InstallSnapshot

					if rf.nextIndex[ind] > rf.lastIncludedIndex {
						// in this case, send out Appendentries

						args := AppendEntriesArgs{}
						args.Term = temp
						args.Timestamp = time.Now()
						args.LeaderID = rf.me
						args.Entries = rf.log[rf.nextIndex[ind]-rf.lastIncludedIndex-1:]
						// Note if Entries is an empty array, then we are sending
						// regular heartbeats
						args.PrevLogIndex = rf.nextIndex[ind] - 1
						if args.PrevLogIndex == rf.lastIncludedIndex {
							args.PrevLogTerm = rf.lastIncludedTerm
						} else {
							args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
						}
						args.LeaderCommit = rf.commitIndex

						reply := AppendEntriesReply{}
						//reply.Follower_disconnect = true
						rf.mu.Unlock()
						rf.peers[ind].Call("Raft.AppendEntriesRPC", &args, &reply)
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.serverState = 0 // convert to follower
							rf.votedFor = -1   // capable of voting in this higher term
							rf.persist()
							rf.mu.Unlock()
							return
						}
						//  need to compare arg.Term with rf.currentTerm
						if args.Term != rf.currentTerm {
							rf.mu.Unlock()
							return
						}
						if reply.Success {
							rf.matchIndex[ind] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[ind] = rf.matchIndex[ind] + 1
							//fmt.Println("line 513", rf.me, ind, reply.Success, reply.CommitIndex)
							for n := rf.commitIndex + 1; n <= len(rf.log)+rf.lastIncludedIndex; n++ {
								count_majority := 1
								for i := 0; i < len(rf.peers); i++ {
									if rf.matchIndex[i] >= n && (i != rf.me) {
										count_majority = count_majority + 1
									}
								}
								if count_majority > len(rf.peers)/2 && rf.log[n-rf.lastIncludedIndex-1].Term == rf.currentTerm {
									rf.commitIndex = n
									rf.persist()
								}
							}

						} else {
							// if failure, there are two possibilities, either prev log
							// entries don't match or the follower gets disconnected
							if reply.Reset_Prev_Index && rf.nextIndex[ind] >= 1 {

								rf.nextIndex[ind] = reply.CommitIndex + 1
								//fmt.Println("line 520", rf.me, ind, reply.Success, reply.CommitIndex)

							} else {

								//fmt.Println("line 524", rf.me, ind, reply.Success, reply.CommitIndex)

								rf.mu.Unlock()
								// follower disconnect in this case, just continue
								return
							}

						}

					} else {

						// else, we need to send out install snapshots

						args2 := InstallSnapshotArgs{}
						reply2 := InstallSnapshotReply{}
						args2.LastIncludedIndex = rf.lastIncludedIndex
						args2.LastIncludedTerm = rf.lastIncludedTerm
						args2.Term = temp
						args2.SnapshotData = rf.snapshotData
						rf.mu.Unlock()
						rf.peers[ind].Call("Raft.InstallSnapshotRPC", &args2, &reply2)
						rf.mu.Lock()
						//  need to compare arg.Term with rf.currentTerm
						if args2.Term != rf.currentTerm {
							rf.mu.Unlock()
							return
						}
						if reply2.Term > rf.currentTerm {
							rf.currentTerm = reply2.Term
							rf.serverState = 0 // convert to follower
							rf.votedFor = -1   // capable of voting in this higher term
							rf.persist()
							rf.mu.Unlock()
							return

						}
						rf.nextIndex[ind] = rf.lastIncludedIndex + 1

					}

					rf.mu.Unlock()

				}(ind)

			}

		}

	} else {
		rf.mu.Unlock()
	}

	return index, term, isleader
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	Timestamp    time.Time  // Timestamp of the RPC call.
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term number at PrevLogIndex entry
	Entries      []logEntry // List of log entries to be added to log; empty for
	// heartbeats
	LeaderCommit int // Highest index known to be committed
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	CommitIndex      int
	Reset_Prev_Index bool
}

func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("line 345", rf.me)

	if args.Term >= rf.currentTerm {
		// update lastRPC as we receive this new call

		rf.lastRPC = args.Timestamp

		// If an AppendEntriesRPC is received from a leader, then
		// we automatically convert to follower's mode, update current term
		// and set votedFor to be -1 if it's not in the same term

		rf.serverState = 0
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.persist()
		}
		rf.currentTerm = args.Term
		rf.persist()

		//fmt.Println(rf.me, args)
		if args.PrevLogIndex < rf.lastIncludedIndex {
			//fmt.Println("Leader's PrevLogIndex < rf.lastIncludedIndex")
			return
		}

		if rf.lastIncludedIndex+len(rf.log) < args.PrevLogIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.CommitIndex = rf.commitIndex
			reply.Reset_Prev_Index = true
			//fmt.Println("line 367", rf.me, len(rf.log))

		} else {
			// else, args.PrevLogIndex <= rf.lastIncludedIndex+len(rf.log)
			var rf_prev_index int
			var rf_prev_term int
			if args.PrevLogIndex == rf.lastIncludedIndex {
				// this indicates len(rf.log) == 0
				rf_prev_index = rf.lastIncludedIndex
				rf_prev_term = rf.lastIncludedTerm
			} else {
				rf_prev_index = args.PrevLogIndex - rf.lastIncludedIndex
				rf_prev_term = rf.log[rf_prev_index-1].Term
			}

			if rf_prev_term != args.PrevLogTerm {
				//fmt.Println("line 371", rf.me, args.PrevLogIndex, rf.log[args.PrevLogIndex], args.PrevLogTerm)
				reply.Term = rf.currentTerm
				reply.Success = false
				// Delete this entry and all the follow it
				reply.CommitIndex = rf.commitIndex
				reply.Reset_Prev_Index = true
				if len(rf.log) != 0 {
					rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex-1]
					//fmt.Println("truncation happened", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
				}
				rf.persist()

			} else {
				//fmt.Println("line 380", rf.me)

				// Else, all the entries up to args.PrevLogIndex are matched
				if len(rf.log)+rf.lastIncludedIndex <= args.PrevLogIndex+len(args.Entries) {
					rf.log = append(rf.log[:args.PrevLogIndex-rf.lastIncludedIndex], args.Entries...)
					//fmt.Println(rf.me, rf.log)
					rf.persist()

				} else {
					for i := 0; i < len(args.Entries); i++ {

						if rf.log[args.PrevLogIndex+i-rf.lastIncludedIndex].Term == args.Entries[i].Term {
							continue
						} else {
							rf.log = append(rf.log[:args.PrevLogIndex+i-rf.lastIncludedIndex], args.Entries[i:]...)
							rf.persist()
							break
						}
					}
				}

				// Reset rf.commitIndex by comparing with leader and the index of last new enrty
				//temp := rf.commitIndex
				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit >= len(rf.log)+rf.lastIncludedIndex {
						rf.commitIndex = len(rf.log) + rf.lastIncludedIndex
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					rf.persist()
				}

				// if rf.commitIndex < 1 {
				// 	fmt.Println("GuiGusShi---------------------------------------------")
				// }

				//fmt.Println("f.me, commitIndex, LastApplied", rf.me, rf.commitIndex, rf.lastApplied)

				// for rf.commitIndex > rf.lastApplied {

				// 	rf.lastApplied = rf.lastApplied + 1
				// 	rf.persist()
				// 	// apply the command to state machine
				// 	apply := ApplyMsg{}
				// 	apply.CommandValid = true
				// 	apply.CommandIndex = rf.lastApplied
				// 	apply.Command = rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command
				// 	rf.mu.Unlock()
				// 	rf.applyCh <- apply
				// 	rf.mu.Lock()

				// 	//fmt.Println("f", rf.me, rf.currentTerm, apply.CommandIndex, apply.Command)

				// }
				//fmt.Println(rf.me, args.PrevLogIndex, args.PrevLogTerm)
				reply.Term = rf.currentTerm
				reply.Success = true

			}

		}

	} else {
		// Else, args.Term < rf.currentTerm, we reply false
		reply.Term = rf.currentTerm
		reply.Success = false

	}

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// We use two separate go routines, one for when the server is leader
		// And another for when the server is not a leader.
		rf.mu.Lock()

		if rf.serverState == 2 {
			temp := rf.currentTerm

			rf.mu.Unlock()

			for ind, _ := range rf.peers {

				if ind != rf.me {

					// then send out corresponding RPC calls to each server concurrently
					go func(ind int) {
						rf.mu.Lock()

						// first, we need to decide if we send AppenEntries or InstallSnapshot

						if rf.nextIndex[ind] > rf.lastIncludedIndex {
							// in this case, send out Appendentries

							args := AppendEntriesArgs{}
							args.Term = temp
							args.Timestamp = time.Now()
							args.LeaderID = rf.me
							args.Entries = rf.log[rf.nextIndex[ind]-rf.lastIncludedIndex-1:]
							// Note if Entries is an empty array, then we are sending
							// regular heartbeats
							args.PrevLogIndex = rf.nextIndex[ind] - 1
							if args.PrevLogIndex == rf.lastIncludedIndex {
								args.PrevLogTerm = rf.lastIncludedTerm
							} else {
								args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
							}
							args.LeaderCommit = rf.commitIndex

							reply := AppendEntriesReply{}
							//reply.Follower_disconnect = true
							rf.mu.Unlock()
							rf.peers[ind].Call("Raft.AppendEntriesRPC", &args, &reply)
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.serverState = 0 // convert to follower
								rf.votedFor = -1   // capable of voting in this higher term
								rf.persist()
								rf.mu.Unlock()
								return
							}
							//  need to compare arg.Term with rf.currentTerm
							if args.Term != rf.currentTerm {
								rf.mu.Unlock()
								return
							}
							if reply.Success {
								rf.matchIndex[ind] = args.PrevLogIndex + len(args.Entries)
								rf.nextIndex[ind] = rf.matchIndex[ind] + 1
								//fmt.Println("line 513", rf.me, ind, reply.Success, reply.CommitIndex)

							} else {
								// if failure, there are two possibilities, either prev log
								// entries don't match or the follower gets disconnected
								if reply.Reset_Prev_Index && rf.nextIndex[ind] >= 1 {

									rf.nextIndex[ind] = reply.CommitIndex + 1
									//fmt.Println("line 520", rf.me, ind, reply.Success, reply.CommitIndex)

								} else {

									//fmt.Println("line 524", rf.me, ind, reply.Success, reply.CommitIndex)

									rf.mu.Unlock()
									// follower disconnect in this case, just continue
									return
								}

							}

						} else {

							// else, we need to send out install snapshots

							args2 := InstallSnapshotArgs{}
							reply2 := InstallSnapshotReply{}
							args2.LastIncludedIndex = rf.lastIncludedIndex
							args2.LastIncludedTerm = rf.lastIncludedTerm
							args2.Term = temp
							args2.SnapshotData = rf.snapshotData
							rf.mu.Unlock()
							rf.peers[ind].Call("Raft.InstallSnapshotRPC", &args2, &reply2)
							rf.mu.Lock()
							//  need to compare arg.Term with rf.currentTerm
							if args2.Term != rf.currentTerm {
								rf.mu.Unlock()
								return
							}
							if reply2.Term > rf.currentTerm {
								rf.currentTerm = reply2.Term
								rf.serverState = 0 // convert to follower
								rf.votedFor = -1   // capable of voting in this higher term
								rf.persist()
								rf.mu.Unlock()
								return

							}
							rf.nextIndex[ind] = rf.lastIncludedIndex + 1

						}

						rf.mu.Unlock()

					}(ind)

				}

			}

			time.Sleep(100 * time.Millisecond)
			// Leader sends heartbeat RPCs no more than ten times per second.
			// so we wait for 100 ms between each hearbeat

			rf.mu.Lock()
			//temp := rf.commitIndex
			//fmt.Println("line 548", temp, rf.commitIndex)
			// update commit index regularly
			for n := rf.commitIndex + 1; n <= len(rf.log)+rf.lastIncludedIndex; n++ {
				count_majority := 1
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] >= n && (i != rf.me) {
						count_majority = count_majority + 1
					}
				}
				if count_majority > len(rf.peers)/2 && rf.log[n-rf.lastIncludedIndex-1].Term == rf.currentTerm {
					rf.commitIndex = n
					rf.persist()
				}
			}

			// apply commited log entries to state machine
			//fmt.Println("Leader.me, term, commitIndex, LastApplied", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)

			// for rf.commitIndex > rf.lastApplied {
			// 	rf.lastApplied = rf.lastApplied + 1
			// 	rf.persist()
			// 	// apply the command to state machine
			// 	apply := ApplyMsg{}
			// 	apply.CommandValid = true
			// 	apply.CommandIndex = rf.lastApplied
			// 	apply.Command = rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command
			// 	rf.mu.Unlock()
			// 	rf.applyCh <- apply
			// 	rf.mu.Lock()
			// 	//fmt.Println("l", rf.me, rf.currentTerm, apply.CommandIndex, apply.Command, rf.log)
			// }
			rf.mu.Unlock()

		} else {
			rf.mu.Unlock()
			// Else, the server is not a leader. Then it will wait to receive RPC calls.
			// If nothing is received after some random interval, the server will start
			// an election
			wait_interval := time.Duration(250+(rand.Int63()%250)) * time.Millisecond
			rf.mu.Lock()
			temp := rf.lastRPC
			rf.mu.Unlock()
			if time.Now().Sub(temp) > wait_interval {

				// the server converts to a candidate
				rf.mu.Lock()
				rf.serverState = 1
				rf.currentTerm = rf.currentTerm + 1
				temp := rf.currentTerm
				rf.votedFor = rf.me
				rf.persist()
				rf.lastRPC = time.Now()
				rf.mu.Unlock()
				countVote := 1
				// send RequestVote RPCs to all other servers

				for ind, _ := range rf.peers {
					if ind != rf.me {
						go func(ind int) {
							rf.mu.Lock()

							args := RequestVoteArgs{}
							args.Timestamp = time.Now()
							args.Term = temp
							args.CandidateId = rf.me
							if len(rf.log) != 0 {
								args.LastLogIndex = len(rf.log) + rf.lastIncludedIndex
								args.LastLogTerm = rf.log[len(rf.log)-1].Term

							} else {
								args.LastLogIndex = rf.lastIncludedIndex
								args.LastLogTerm = rf.lastIncludedTerm
							}
							//fmt.Println("candidate, rf.me, rf.Term, args.LastLogIndex, args.LastLogTerm", rf.me, rf.currentTerm, args.LastLogIndex, args.LastLogTerm)

							reply := RequestVoteReply{}
							rf.mu.Unlock()
							rf.peers[ind].Call("Raft.RequestVote", &args, &reply)
							rf.mu.Lock()

							if args.Term != rf.currentTerm {

								rf.mu.Unlock()
								return

							}
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.serverState = 0 // convert to follower
								rf.votedFor = -1   // now we are able to vote
								// as a follower in higehr term
								rf.persist()
								countVote = (-1) * len(rf.peers)
								// this disqualifies the candidate of being selected leader
							}
							if reply.Votegranted {
								countVote = countVote + 1

							}

							rf.mu.Unlock()

						}(ind)

					}
				}
				time.Sleep(100 * time.Millisecond)

				rf.mu.Lock()

				if countVote > len(rf.peers)/2 {
					// Elected to be leader
					rf.serverState = 2
					//fmt.Println("leader changed", rf.me, rf.currentTerm, len(rf.log))
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}

				}
				rf.mu.Unlock()

			}
			//fmt.Println("f commitIndex", rf.me, rf.commitIndex)

			time.Sleep(100 * time.Millisecond)

		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.lastRPC = time.Now()
	rf.votedFor = -1
	rf.serverState = 0

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []logEntry{}
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshotData = rf.persister.ReadSnapshot()
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine
	go rf.ticker()
	// start another background goroutine to keep sending commands to applyChan
	// once Raft peers have reached agreements
	go func() {
		for {
			rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied {

				rf.lastApplied = rf.lastApplied + 1
				rf.persist()
				// apply the command to state machine
				apply := ApplyMsg{}
				apply.CommandValid = true
				apply.CommandIndex = rf.lastApplied
				apply.Command = rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command
				apply.CommandTerm = rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Term
				rf.mu.Unlock()
				rf.applyCh <- apply
				rf.mu.Lock()

				//fmt.Println("Apply Goroutine, rf.me, rf.currentTerm, CommandIndex, Command", rf.me, rf.currentTerm, apply.CommandIndex, apply.Command)

			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}()

	return rf
}
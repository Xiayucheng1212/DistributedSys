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
	// "fmt"
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

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntries struct {
	Term         int // leader term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm	int // term in the conflicting entry (if any)
	XIndex	int // index of the first entry in the conflicting term (if any)
	XLen	int // log length
}

func (rf *Raft) ClearSettings() {
	rf.state = 0 // Received a heartbeat, so the server is a follower
	rf.votedFor = -1
	rf.votesReceived = 0
	rf.electionTimer = time.Now() // reset the election timer
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) FindStartIndex(XTerm int, startIdx int) int {
	for i := startIdx; i >= 0; i-- {
		if rf.log[i].Term != XTerm {
			return i + 1
		}
	}

	return 0
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	fmt.Printf("Server %d received append entries from leader %d with args PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d, len(args.Entries): %d\n", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// As long as receiving the heartbeat, the server should become a follower
	reply.XTerm = -1 // if any
	reply.XIndex = -1 // if any
	reply.XLen = len(rf.log)
	reply.Term = rf.currentTerm
	rf.currentTerm = args.Term
	rf.ClearSettings()

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// If the term of the log entry does not match, delete the log entry and all the following entries
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = rf.FindStartIndex(reply.XTerm, args.PrevLogIndex)
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Success = false
	} else {
		// If the term of the log entry matches, append the new entries
		if len(args.Entries) > 0 {
			rf.log = rf.log[:args.PrevLogIndex + 1]
			rf.log = append(rf.log, args.Entries...)
		}
		reply.Success = true
	}

	// If the leader commit index is greater than the server's commit index, update the commit index
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	// If the append entries are successful, check the commitIndex > lastApplied, do apply to log[lastApplied] and increment lastApplied
	if rf.commitIndex > rf.lastApplied && reply.Success {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			if i >= len(rf.log) {
				break
			}
			
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.lastApplied = i
			// Send the applyMsg to the applyCh
			fmt.Printf("applyMsg command: %v, i : %v, commitIndex: %v\n", applyMsg.Command, i, rf.commitIndex)
			rf.applyCh <- applyMsg
		}
	}

	// fmt.Printf("Server %d persists log: %v\n", rf.me, rf.log)
}

func (rf *Raft) sendAppendRPCRepeatedly(server int, args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	args.LeaderCommit = rf.commitIndex
	args.Term = rf.currentTerm
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = rf.log[args.PrevLogIndex+1:]

	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	fmt.Printf("args.PrevLogIndex: %v, nextIndex[%v]: %v\n", args.PrevLogIndex, server, rf.nextIndex[server])
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()
	fmt.Printf("Server %d sending append entries to server %d with PrevLogIndex: %d, PrevLogTerm: %d, entries: %v, rf.nextIndex[%d]: %d, ok: %v, rf.currentTerm: %d\n", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, args.Entries, server, rf.nextIndex[server], ok, rf.currentTerm)
	rf.mu.Unlock()

	rf.mu.Lock()
	if reply.Success && ok {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for !rf.killed() && !ok && rf.state == 2 {
		// Notice: disconnect means rf.killed() is true
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.ClearSettings()
			rf.mu.Unlock()
			fmt.Printf("Server %d received higher term from server %d, currentTerm: %d in send append rpc\n", rf.me, server, rf.currentTerm)
			return
		}
		args.LeaderCommit = rf.commitIndex
		args.Term = rf.currentTerm
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = rf.log[args.PrevLogIndex+1:]
		rf.mu.Unlock()
		ok = rf.sendAppendEntries(server, args, reply)
		rf.mu.Lock()
		fmt.Printf("Server %d re-sending append entries to server %d with PrevLogIndex: %d, PrevLogTerm: %d, len(rf.log):%v, rf.nextIndex[%d]: %d\n", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), server, rf.nextIndex[server])
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) setNextIndex(server int, reply AppendEntriesReply, startIdx int) {
	// if follower's log length is shorter
	if reply.XLen < len(rf.log) {
		rf.nextIndex[server] = reply.XLen
		return
	}
	
	// find if leader has XTerm
	hasXTerm := false
	lastEntry := startIdx
	for i := startIdx; i >= 0; i-- {
		if rf.log[i].Term == reply.XTerm {
			hasXTerm = true
			lastEntry = i
			break
		}
	}

	if !hasXTerm {
		rf.nextIndex[server] = reply.XIndex
	} else {
		rf.nextIndex[server] = lastEntry + 1 // leader's last entry for XTerm + 1
	}
}

func (rf *Raft) sendReplicateRequest(server int) {
	rf.mu.Lock()

	args := AppendEntries{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      rf.log[rf.nextIndex[server]:],
		LeaderCommit: rf.commitIndex, // To let the followers know the commitIndex
	}

	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

	rf.mu.Unlock()
	reply := AppendEntriesReply{Success: false, Term: 0}

	// 1. Send the append entries RPC until it is successful
	rf.sendAppendRPCRepeatedly(server, &args, &reply)
	// 2. Reduce the nextIndex and retry if the append entries is not successful
	for !rf.killed() && !reply.Success && rf.state == 2 {
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.ClearSettings()
			rf.mu.Unlock()
			fmt.Printf("Server %d received higher term from server %d, currentTerm: %d in replicate request\n", rf.me, server, rf.currentTerm)
			return
		} else {
			rf.mu.Lock()
			rf.setNextIndex(server, reply, args.PrevLogIndex)
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if (args.PrevLogIndex < 0) {
				fmt.Printf("Reply of XTerm: %d, Xindex: %d, XLen: %d, rf.nextIndex[server]: %d\n", reply.XTerm, reply.XIndex, reply.XLen, rf.nextIndex[server])
				fmt.Printf("Server %d in state %d retrying append entries to server %d with PrevLogIndex: %d\n", rf.me, rf.state, server, args.PrevLogIndex)
			}
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[args.PrevLogIndex+1:]
			args.Term = rf.currentTerm
			fmt.Printf("Server %d in state %d retrying append entries to server %d with PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v\n", rf.me, rf.state, server, args.PrevLogIndex, args.PrevLogTerm, args.Entries)

			rf.mu.Unlock()
			rf.sendAppendRPCRepeatedly(server, &args, &reply)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// 3. If the append entries is successful, update the nextIndex and matchIndex
	rf.mu.Lock()
	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		fmt.Printf("Server %d successfully replicated to server %d, rf.nextIndex[%d]: %d, rf.matchIndex[%d]: %d\n", rf.me, server, server, rf.nextIndex[server], server, rf.matchIndex[server])
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeats(server int) {
	rf.mu.Lock()
	args := AppendEntries{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}

	// For heartbeats, the PrevLogIndex and PrevLogTerm are set to -1 and 0
	args.PrevLogIndex = len(rf.log) - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

	rf.mu.Unlock()
	reply := AppendEntriesReply{}

	// Notice: Must not hold the lock when sending RPC
	rf.mu.Lock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// Notice: due to bad network, the response time could be long
	ok := rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	fmt.Printf("Server %d sending heartbeats to server %d, len(rf.log): %v\n", rf.me, server, len(rf.log))
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.ClearSettings()
			fmt.Printf("Server %d received higher term from server %d, currentTerm: %d in send heart beat\n", rf.me, server, rf.currentTerm)
		}
	}
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
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	state           int // 0: follower, 1: candidate, 2: leader
	votesReceived   int
	electionTimer   time.Time
	electionTimeout time.Duration
	applyCh         chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == 2

	return term, isleader
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
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Printf("Error in decoding the data\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) isUpToDate(lastLogIndex, lastLogTerm int) bool {
	if len(rf.log) == 0 {
		return true
	}
	lastTerm := rf.log[len(rf.log)-1].Term
	if lastTerm != lastLogTerm {
		return lastTerm < lastLogTerm
	}
	return len(rf.log)-1 <= lastLogIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = 0
		rf.currentTerm = args.Term
		rf.votedFor = -1 // Vote again in the new term as the server is a follower
		rf.votesReceived = 0
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}
	}

	// if the candidate's log is not up-to-date, reject the vote
	// if the server is already voted for another candidate, reject the vote
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	serverInstance := rf.peers[server]
	rf.persist()
	rf.mu.Unlock()
	ok := serverInstance.Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()

	if rf.state == 2 {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true
		rf.mu.Unlock()

		rf.mu.Lock()
		fmt.Printf("Server %d starting append entries\n", rf.me)
		rf.mu.Unlock()

		go func() {
			rf.startAppendEntries()

			rf.mu.Lock()

			// No need to check the state, cause even the previous leader has been downgraded
			// the majority of the servers have acknowledged the append.
			if !rf.killed() {
				rf.commitIndex = index
				rf.matchIndex[rf.me] = index
				rf.nextIndex[rf.me] = index + 1
	
				fmt.Printf("Server %d finished waiting for majority of the servers, len(rf.log): %v, rf.commitIndex: %d\n", rf.me, len(rf.log), rf.commitIndex)
			}
			
			// Apply Msg after update the commitIndex
			if rf.commitIndex > rf.lastApplied {
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
					rf.lastApplied = i
					// Send the applyMsg to the applyCh
					rf.applyCh <- applyMsg
				}
			}
			isLeader = rf.state == 2
			rf.mu.Unlock()
		}()
	} else {
		isLeader = false
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return index, term, isLeader
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

func (rf *Raft) startAppendEntries() {

	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		if i != rf.me && rf.state == 2 {
			rf.mu.Unlock()
			go func(server int) {
				rf.sendReplicateRequest(server)
			}(i)
		} else {
			rf.mu.Unlock()
		}
	}
	// only wait for the majority of the servers if the server is still a leader
	for !rf.killed() {
		// check whether the number of matchIndex is greater than the majority of the servers
		count := 0
		rf.mu.Lock()

		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= len(rf.log)-1 {
				count++
			}
		}

		if count > len(rf.peers)/2-1 { // the minus 1 here is because we don't commit the server yet
			fmt.Printf("Jump out the majority waiting loop\n")
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.state = 1
	rf.electionTimer = time.Now() // reset the election timer, when there is no server becomes a leader, the election timer will timeout

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
	}

	if len(rf.log) > 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		args.LastLogTerm = 0
	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		if i != rf.me && rf.state == 1 {
			rf.mu.Unlock()
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// If the server does not respond properly, do nothing
				if !ok {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.ClearSettings()
					return
				}

				if reply.VoteGranted {
					rf.votesReceived++
					if rf.votesReceived > len(rf.peers)/2 {
						rf.state = 2
						rf.votesReceived = 0
						// initialize nextIndex and matchIndex with the length of the log
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						fmt.Printf("Server %d becomes leader\n", rf.me)
					}
				}
			}(i)
		} else {
			if rf.votesReceived > len(rf.peers)/2 {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.state != 2 {
			if time.Since(rf.electionTimer) > rf.electionTimeout {
				rf.mu.Unlock()
				fmt.Printf("Server %d starting election\n", rf.me)
				rf.startElection()

				rf.mu.Lock()
				// If lose the election, the server should wait for the election timeout
				rf.electionTimer = time.Now()
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				// send heartbeats
				rf.mu.Lock()
				if i != rf.me && rf.state == 2 {
					rf.mu.Unlock()
					go rf.sendHeartBeats(i)
				} else {
					rf.mu.Unlock()
				}
			}
			time.Sleep(100 * time.Millisecond) // heartbeats no more than 10 times per second
		}
		time.Sleep(10 * time.Millisecond)
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
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil}) // index starts from 1
	rf.commitIndex = 0                                       // volatile state on all servers
	rf.lastApplied = 0                                       // volatile state on all servers
	rf.nextIndex = make([]int, len(peers))                   // volatile state on leaders
	rf.matchIndex = make([]int, len(peers))                  // volatile state on leaders
	rf.state = 0
	rf.votesReceived = 0
	rf.electionTimer = time.Now()
	ms := 300 + (rand.Int63() % 150)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

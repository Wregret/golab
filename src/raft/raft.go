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
	"bytes"
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntries
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int // -1 means nil
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders: Reinitialized after election
	nextIndex  []int
	matchIndex []int

	// Extra
	status       int           // FOLLOWER, CANDIDATE, LEADER
	resetTimeout chan struct{} // chan to reset timeout

	// To upper layer service (k-v storage)
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("[RAFT %d]: Reboot...", rf.me)
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ct int
	var vf int
	l := make([]LogEntry, 0)
	err := d.Decode(&ct)
	if err != nil {
		DPrintf(err.Error())
	} else {
		rf.currentTerm = ct
		DPrintf("[RAFT %d]currentTerm: --- %d ---", rf.me, ct)
	}
	err = d.Decode(&vf)
	if err != nil {
		DPrintf(err.Error())
	} else {
		rf.votedFor = vf
		DPrintf("[RAFT %d]votedFor: --- %d ---", rf.me, vf)
	}
	err = d.Decode(&l)
	if err != nil {
		DPrintf(err.Error())
	} else {
		rf.log = l
		DPrintf("[RAFT %d]len(log): --- %d ---", rf.me, len(rf.log))
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[RAFT %d][TERM %d]: RequestVoteArgs {Term: %d, candidateId: %d}", rf.me, rf.currentTerm, args.Term, args.CandidateId)
	// All Server Rule 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = FOLLOWER
	}
	// Set reply term
	reply.Term = rf.currentTerm
	// Receiver rules 1
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// Receiver rules 2
	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIndex].Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId // change votedFor
		rf.resetTimeout <- struct{}{}  // reset timeout
		DPrintf("[RAFT %d][TERM %d]: RequestVoteReply {Term: %d, VoteGranted: %t}", rf.me, rf.currentTerm, reply.Term, reply.VoteGranted)
		return
	} else {
		reply.VoteGranted = false
		return
	}
}

//
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
//
func (rf *Raft) sendRequestVoteExample(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote() {
	rf.mu.Lock()
	DPrintf("[RAFT %d][TERM %d]: Start election", rf.me, rf.currentTerm)
	// step up to CANDIDATE
	rf.currentTerm += 1
	rf.status = CANDIDATE
	rf.votedFor = rf.me
	rf.persist()
	rf.resetTimeout <- struct{}{}
	// snapshot of the current state
	ctm := rf.currentTerm   // current term
	lli := len(rf.log) - 1  // last log index
	llt := rf.log[lli].Term // last log term
	rf.mu.Unlock()

	// handle election
	var electionMu sync.Mutex
	cond := sync.NewCond(&electionMu)
	voteCount := 0
	voteFinish := 0
	for id := range rf.peers {
		// skip myself
		if id == rf.me {
			continue
		}
		// send RequestVote RPC
		go func(currentTerm int, lastLogTerm int, lastLogIndex int, from int, to int) {
			defer cond.Broadcast()
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  from,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if rf.peers[to].Call("Raft.RequestVote", &args, &reply) {
				rf.mu.Lock()
				// All Server Rule 2
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.status = FOLLOWER
					rf.persist()
				}
				// drop old RPC reply
				if currentTerm < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				// modify election record
				electionMu.Lock()
				defer electionMu.Unlock()
				voteFinish += 1
				if reply.VoteGranted {
					voteCount += 1
				}
			}
		}(ctm, llt, lli, rf.me, id)
	}

	// count vote
	electionMu.Lock()
	majority := len(rf.peers) / 2
	for voteCount < majority && voteFinish != len(rf.peers)-1 && !rf.killed() {
		cond.Wait()
	}
	electionMu.Unlock()
	//DPrintf("[RAFT %d][TERM %d]: Election finish. Get %d votes", rf.me, rf.currentTerm, voteCount)

	// compare with snapshot and finish election
	rf.mu.Lock()
	if voteCount >= majority &&
		rf.status == CANDIDATE &&
		rf.votedFor == rf.me &&
		ctm == rf.currentTerm &&
		lli == len(rf.log)-1 &&
		llt == rf.log[lli].Term {
		// LEADER up
		rf.status = LEADER
		DPrintf("[RAFT %d][TERM %d]: =====LEADER!!===== len(rf.log)=%d", rf.me, rf.currentTerm, len(rf.log))
		for idx := range rf.nextIndex {
			rf.nextIndex[idx] = len(rf.log) // leader last log index + 1
			rf.matchIndex[idx] = 0
		}
		go rf.heartBeat()
		go rf.checkCommit()
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term == rf.currentTerm {
		rf.resetTimeout <- struct{}{}
	}

	if len(args.Entries) != 0 {
		DPrintf("[RAFT %d][TERM %d]: AppendEntriesArgs {Term: %d, leaderId: %d, PLI: %d, PLT: %d, len(Entries): %d, leaderCommit: %d}", rf.me, rf.currentTerm, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	}
	// All Server Rule 2
	if args.Term > rf.currentTerm {
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// Set reply term
	reply.Term = rf.currentTerm
	// Receiver rules 1
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// Receiver rules 2 with log backtracking optimization
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term{
		reply.Success = false
		DPrintf("[RAFT %d][TERM %d]: Reject AE PLI: %d, len(rf.log): %d", rf.me, rf.currentTerm, args.PrevLogIndex, len(rf.log))
		return
	}
	// Receiver rules 3
	cursor := 0
	for ; cursor < len(args.Entries) && cursor+args.PrevLogIndex+1 < len(rf.log); cursor++ {
		if rf.log[args.PrevLogIndex+1+cursor].Term != args.Entries[cursor].Term {
			rf.log = rf.log[:args.PrevLogIndex+1+cursor]
			break
		}
	}
	// Receiver rules 4
	rf.log = append(rf.log, args.Entries[cursor:]...)

	// Receiver rules 5
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[RAFT %d][TERM %d]: leaderCommit: %d, commitIndex: %d", rf.me, rf.currentTerm, args.LeaderCommit, rf.commitIndex)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	}
	DPrintf("[RAFT %d][TERM %d]: Do I apply? len(log): %d, lastApplied: %d, commitIndex: %d", rf.me, rf.currentTerm, len(rf.log), rf.lastApplied, rf.commitIndex)
	rf.apply()
	reply.Success = true
	if len(args.Entries) != 0 {
		DPrintf("[RAFT %d][TERM %d]: AppendEntriesReply {Term: %d, Success: %t}", rf.me, rf.currentTerm, reply.Term, reply.Success)
	}
	return
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != LEADER {
		return
	}
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go rf.sendAppendEntriesTo(id)
	}
}

func (rf *Raft) sendAppendEntriesTo(to int) {
	rf.mu.Lock()
	if rf.status != LEADER {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	prevLogIndex := rf.nextIndex[to] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	var entries []LogEntry
	entries = append(entries, rf.log[prevLogIndex+1:]...)
	leaderCommit := rf.commitIndex
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	DPrintf("[RAFT %d][TERM %d]: AE sent to %d{PLI: %d, PLT: %d, len(entries): %d, leaderCommit: %d}", rf.me, rf.currentTerm, to, prevLogIndex, prevLogTerm, len(entries), leaderCommit)
	rf.mu.Unlock()
	if rf.peers[to].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		// All Server Rule 2 -- fail not because of log inconsistency
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.status = FOLLOWER
			rf.persist()
			rf.mu.Unlock()
			return
		}
		// drop old RPC
		if currentTerm != rf.currentTerm || prevLogIndex != rf.nextIndex[to]-1 {
			rf.mu.Unlock()
			return
		}
		// AppendEntries Success
		if reply.Success {
			rf.matchIndex[to] = int(math.Max(float64(rf.matchIndex[to]), float64(prevLogIndex+len(entries))))
			rf.nextIndex[to] = rf.matchIndex[to] + 1
			if len(args.Entries) != 0 {
				DPrintf("[RAFT %d][TERM %d]:AppendEntries to %d successed matchIndex[%d] = %d, nextIndex[%d] = %d", rf.me, rf.currentTerm, to, to, rf.matchIndex[to], to, rf.nextIndex[to])
			}
			// Leaders Rule 4
			rf.checkCommit()
			rf.apply()
			rf.mu.Unlock()
			return
		}
		// AppendEntries fail because of log
		rf.nextIndex[to] = int(math.Max(float64(rf.nextIndex[to]/2), 1))
		DPrintf("[RAFT %d][TERM %d]: AppendEntries to %d failed. ", rf.me, rf.currentTerm, to)
		go rf.sendAppendEntriesTo(to)
		rf.mu.Unlock()
		return
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	if rf.killed() {
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.status == LEADER

	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		DPrintf("[RAFT %d][TERM %d]: ~~~~~~ Client Request in index: %d ~~~~~~", rf.me, rf.currentTerm, len(rf.log)-1)
		rf.persist()
		go rf.sendAppendEntries()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      nil,
		CommandIndex: 0,
	}

	// Your initialization code here (2A, 2B, 2C).
	// struct initialization
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.status = FOLLOWER
	rf.resetTimeout = make(chan struct{})
	for i := range peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("[RAFT %d][TERM: %d]: Reboot......", rf.me, rf.currentTerm)

	// kick off countdown
	go rf.timeout()

	return rf
}

func (rf *Raft) timeout() {
	for !rf.killed() {
		rand.Seed(time.Now().UnixNano())
		timer := time.NewTimer(time.Duration(300+rand.Intn(500)) * time.Millisecond)
		select {
		case <-rf.resetTimeout:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			rf.mu.Lock()
			if rf.status != LEADER {
				go rf.sendRequestVote()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status != LEADER {
			rf.mu.Unlock()
			return
		}
		DPrintf("[RAFT %d][TERM %d]: +++ Heart Beat! +++", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		go rf.sendAppendEntries()
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) apply() {
	// Please make sure rf is LOCKED before calling this function
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("[RAFT %d][TERM %d]: APPLY commitIndex: %d, command: %d", rf.me, rf.currentTerm, rf.lastApplied, rf.log[rf.lastApplied].Command)
	}
}

func (rf *Raft) checkCommit() {
	// Please make sure rf is LOCKED before calling this function
	majority := len(rf.peers) / 2
	for N := rf.commitIndex + 1; N < len(rf.log); N++ {
		if rf.log[N].Term != rf.currentTerm {
			DPrintf("[RAFT %d][TERM %d]: Leader check commit N skip: N: %d, NTerm: %d", rf.me, rf.currentTerm, N, rf.log[N].Term)
			continue
		}
		count := 0
		for idx := range rf.matchIndex {
			if idx == rf.me {
				continue
			}
			if rf.matchIndex[idx] >= N {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = N
			DPrintf("[RAFT %d][TERM %d]: Leader commit to: %d", rf.me, rf.currentTerm, rf.commitIndex)
		}
	}
}

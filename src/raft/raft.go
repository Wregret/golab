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
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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
	commitCond   *sync.Cond

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
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
				// drop old RPC reply
				if currentTerm != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				// All Server Rule 2
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.status = FOLLOWER
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
		go rf.committer()
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	// Receiver rules 2
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
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
	// Applier
	if rf.commitIndex > rf.lastApplied {
		// TODO: Apply log
		for index := rf.lastApplied + 1; index < rf.commitIndex+1; index++ {
			DPrintf("[RAFT %d][TERM %d]: APPLY commitIndex: %d, command: %d", rf.me, rf.currentTerm, index, rf.log[index].Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[index].Command,
				CommandIndex: index,
			}
		}
		rf.lastApplied = rf.commitIndex
	}
	rf.resetTimeout <- struct{}{}
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
		pli := rf.nextIndex[id] - 1
		plt := rf.log[pli].Term
		var ent []LogEntry
		ent = append(ent, rf.log[pli+1:]...)
		lc := rf.commitIndex
		DPrintf("[RAFT %d][TERM %d]: sendAE: pli: %d, nextIndex[%d]: %d, len(entries): %d, commitedIndex: %d", rf.me, rf.currentTerm, pli, id, rf.nextIndex[id], len(ent), rf.commitIndex)
		go func(currentTerm int, from int, to int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) {
			for !rf.killed() {
				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     from,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				reply := AppendEntriesReply{}
				if rf.peers[to].Call("Raft.AppendEntries", &args, &reply) {
					// drop old RPC
					rf.mu.Lock()
					if currentTerm < rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					// All Server Rule 2 -- fail not because of log inconsistency
					if reply.Term > rf.currentTerm || (reply.Term > currentTerm && !reply.Success) {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.status = FOLLOWER
						rf.mu.Unlock()
						return
					}
					// AppendEntries Success
					if reply.Success {
						rf.matchIndex[to] = pli + len(ent)
						rf.nextIndex[to] = rf.matchIndex[to] + 1
						if len(args.Entries) != 0 {
							DPrintf("[RAFT %d][TERM %d]:AppendEntries successed matchIndex[%d] = %d, nextIndex[%d] = %d", rf.me, rf.currentTerm, to, rf.matchIndex[to], to, rf.nextIndex[to])
						}
						rf.mu.Unlock()
						rf.commitCond.Broadcast()
						return
					}
					// AppendEntries fail because of log inconsistency
					rf.nextIndex[to] -= 1
					prevLogIndex = rf.nextIndex[to] - 1
					prevLogTerm = rf.log[prevLogIndex].Term
					entries = []LogEntry{}
					entries = append(entries, rf.log[prevLogIndex+1:]...)
					DPrintf("[RAFT %d][TERM %d]: AppendEntries failed. Next try: pli: %d, len(entries): %d", rf.me, rf.currentTerm, prevLogIndex, len(entries))
					rf.mu.Unlock()
				}
			}
		}(rf.currentTerm, rf.me, id, pli, plt, ent, lc)
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
	rf.commitCond.Broadcast()
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
	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.status = FOLLOWER
	rf.resetTimeout = make(chan struct{})
	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
		rf.mu.Unlock()
		go rf.sendAppendEntries()
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) committer() {
	rf.mu.Lock()
	for !rf.killed() && rf.status == LEADER {
		majority := len(rf.peers) / 2
		for N := rf.commitIndex + 1; N < len(rf.log); N++ {
			count := 0
			for _, mi := range rf.matchIndex {
				if mi >= N {
					count++
				}
			}
			if N < len(rf.log) && rf.log[N].Term == rf.currentTerm && count >= majority {
				rf.commitIndex = N
			}
		}
		// Applier
		if rf.commitIndex > rf.lastApplied {
			// TODO: Apply log
			for index := rf.lastApplied + 1; index < rf.commitIndex+1; index++ {
				DPrintf("[RAFT %d][TERM %d]: (committer) APPLY commitIndex: %d, command: %d", rf.me, rf.currentTerm, index, rf.log[index].Command)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[index].Command,
					CommandIndex: index,
				}
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.commitCond.Wait()
	}
	rf.mu.Unlock()
}

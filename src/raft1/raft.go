package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"raft/labgob"
	"raft/labrpc"
	"raft/raftapi"
	"raft/tester1"
	"sync"
	"sync/atomic"
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mutex     sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	majority  int                 // a constant after init
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         Log

	state         StateMachine
	electionTimer *ElectionTimer // only wait in tickerFunc, so thread-safe

	commitIndex int
	lastApplied int
	rollback    []int
	nextIndex   []int
	matchIndex  []int

	heartbeatCh []chan bool
	commitMu    sync.Mutex
	commitCond  *sync.Cond

	leaderState    LeaderState
	followerState  FollowerState
	candidateState CandidateState
}

type ServerState int

const (
	ST_Leader ServerState = iota
	ST_Follower
	ST_Candidate
)

type StateMachine interface {
	getState() ServerState
	tickerFunc(rf *Raft)
}

func (rf *Raft) stateName() string {
	switch rf.state.getState() {
	case ST_Leader:
		return "Leader"
	case ST_Follower:
		return "Follower"
	case ST_Candidate:
		return "Candidate"
	}
	return "Unknown"
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	return rf.currentTerm, rf.state.getState() == ST_Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(rf.currentTerm) != nil ||
		encoder.Encode(rf.votedFor) != nil ||
		encoder.Encode(rf.log) != nil {
		println("persist err")
		return
	}
	raftstate := buffer.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	var pCurrentTerm, pVoteFor int
	var pLog Log
	if decoder.Decode(&pCurrentTerm) != nil ||
		decoder.Decode(&pVoteFor) != nil ||
		decoder.Decode(&pLog) != nil {
		return
	} else {
		rf.currentTerm = pCurrentTerm
		rf.votedFor = pVoteFor
		rf.log = pLog
	}
}

// PersistBytes how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// should lock before call it
func (rf *Raft) termCheck(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
		rf.state = &rf.followerState
		rf.electionTimer.resetTimer(rf)
		rf.dprint("term check - switch to follower")
	}
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	// currentTerm can only increase, so no lock here
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	rf.termCheck(args.Term)
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		// Election restriction: at least as up-to-date check
		if (args.LastLogTerm > rf.log.LastTerm) ||
			(args.LastLogTerm == rf.log.LastTerm && args.LastLogIndex >= rf.log.LastIndex) {
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.dprint(fmt.Sprintf("vote granted to %d", rf.votedFor))
		}
	}
	reply.VoteGranted = rf.votedFor == args.CandidateId
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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.mutex.Lock()

	if rf.state.getState() == ST_Candidate && args.Term == rf.currentTerm {
		rf.state = &rf.followerState
		//rf.dprint("append same term - switch to follower")
	} else {
		rf.termCheck(args.Term)
	}
	rf.dprintf3C("receive heartbeat from %d", args.LeaderId)
	rf.electionTimer.resetTimer(rf)

	reply.Term = rf.currentTerm
	if rf.log.LastIndex < args.PrevLogIndex {
		reply.XTerm = -1
		reply.XLen = rf.log.LastIndex + 1
		reply.Success = false
	} else if rf.log.Entries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log.Entries[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for rf.log.Entries[i].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		reply.XLen = rf.log.LastIndex + 1
		reply.Success = false
	} else {
		change, lastNewIndex := rf.log.followerAppend(args.Entries, args.PrevLogIndex, rf)
		if change {
			rf.persist()
		}
		// can't update commitIndex if it haven't kept up with new leader
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(lastNewIndex, args.LeaderCommit)
			//rf.dassert(newCommitIndex >= rf.commitIndex, "New commit index is greater than commit index")
			if newCommitIndex > rf.commitIndex {
				rf.commitMu.Lock()
				rf.commitIndex = newCommitIndex
				rf.commitCond.Signal()
				rf.commitMu.Unlock()
			}
		}
		reply.Success = true
	}
	rf.mutex.Unlock()
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
// within a candidateTimeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

const maxEntrySend = 64

func (rf *Raft) heartbeatOnce(server int) {
	// Use lock here to prevent this server switch to not-leader state.
	// Ensure the Term in args is the currentTerm when it believes it is
	// leader.
	rf.mutex.Lock()
	if rf.state.getState() != ST_Leader {
		rf.mutex.Unlock()
		return
	}
	nextId := rf.nextIndex[server]
	rollback := rf.rollback[server]
	if rollback == 0 && rf.log.LastIndex >= nextId {
		rollback = min(2*maxEntrySend, rf.log.LastIndex+1-nextId)
	}
	args := AppendEntriesArgs{
		Term: rf.currentTerm, LeaderId: rf.me,
		PrevLogIndex: nextId - 1,
		PrevLogTerm:  rf.log.Entries[nextId-1].Term,
		Entries:      rf.log.Entries[nextId : nextId+rollback],
		LeaderCommit: rf.commitIndex}
	rf.mutex.Unlock()

	reply := AppendEntriesReply{}
	rf.dprintf3C("sent heartbeat to %d", server)
	defer rf.dprintf3C("finished heartbeat to %d", server)
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return
	}
	rf.mutex.Lock()
	rf.termCheck(reply.Term)
	if rf.state.getState() != ST_Leader {
		rf.mutex.Unlock()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = nextId + rollback
		rf.rollback[server] = 0
		// more than one
		//rf.rollback[server] = min(maxEntrySend, rf.log.LastIndex+1-rf.nextIndex[server])
		//println("next rollback ", rf.rollback[server])
		//rf.rollback[server] = min(1, rf.log.LastIndex+1-rf.nextIndex[server])
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.dprintf3C("new matchIndex %d from %d, my commitIndex %d", rf.matchIndex[server], server, rf.commitIndex)
		rf.updateCommitIndex(rf.matchIndex[server])
		//if rf.log.LastIndex >= rf.nextIndex[server] {
		//	select {
		//	case rf.heartbeatCh[server] <- true:
		//	default:
		//	}
		//}
	} else {
		rollback = max(min(maxEntrySend, rf.rollback[server]*2), 2)
		rollback = min(rollback, rf.nextIndex[server]-1)
		rf.rollback[server] = rollback
		newNextIndex := 1
		if reply.XTerm < 0 {
			newNextIndex = reply.XLen
		} else {
			//rf.dassert(reply.XTerm < args.PrevLogTerm, "XTerm larger than PrevLogTerm")
			i := args.PrevLogIndex - 1
			for rf.log.Entries[i].Term > reply.XTerm {
				i -= 1
			}
			if rf.log.Entries[i].Term == reply.XTerm {
				newNextIndex = i + 1
			} else {
				newNextIndex = reply.XIndex
			}
		}
		rf.nextIndex[server] = min(rf.nextIndex[server]-rollback, newNextIndex)
		//select {
		//case rf.heartbeatCh[server] <- true:
		//default:
		//}
	}
	//else {
	//	rollback = max(min(maxEntrySend, rf.rollback[server]*2), 2)
	//	rollback = min(rollback, rf.nextIndex[server]-1)
	//	//rollback := 1
	//	rf.rollback[server] = rollback
	//	rf.nextIndex[server] = min(rf.nextIndex[server]-rollback, reply.XLen)
	//	select {
	//	case rf.heartbeatCh[server] <- true:
	//	default:
	//	}
	//}
	rf.mutex.Unlock()
}

func (rf *Raft) heartbeatThread(server int) {
	for rf.killed() == false {
		select {
		case <-rf.heartbeatCh[server]:
			rf.dprintf3C("call sent heartbeat to %d", server)
			rf.heartbeatOnce(server)
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go rf.heartbeatOnce(server)
		//select {
		//case rf.heartbeatCh[server] <- true:
		//	//rf.dprintf3C("fill heartbeat channel to %d, success", server)
		//default:
		//	//rf.dprintf3C("fill heartbeat channel to %d, fail", server)
		//}
		//rf.dprintf3C("fill heartbeat channel to %d", server)
	}
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if rf.state.getState() != ST_Leader {
		return 0, 0, false
	}
	term := rf.currentTerm
	index := rf.log.leaderAppend(LogEntry{Term: term, Command: command}, rf)
	rf.persist()
	return index, term, true
}

// Kill
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
		rf.state.tickerFunc(rf)
	}
}

// Make
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.majority = len(peers) / 2
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 1 // init term=1, because term=0 stands for empty log-entry
	rf.votedFor = -1

	// initialize log
	rf.log.Entries = make([]LogEntry, 0)
	rf.log.Entries = append(rf.log.Entries, LogEntry{Term: 0})
	rf.log.LastIndex = 0
	rf.log.LastTerm = 0

	rf.state = &rf.followerState
	rf.electionTimer = makeElectionTimer()

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.rollback = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.heartbeatCh = make([]chan bool, len(rf.peers))
	for i := 0; i < len(peers); i++ {
		if i == rf.me {
			continue
		}
		rf.heartbeatCh[i] = make(chan bool, 2)
		//go rf.heartbeatThread(i)
	}

	rf.commitCond = sync.NewCond(&rf.commitMu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start commit-command goroutine
	go rf.commitThread()

	return rf
}

func (rf *Raft) commitThread() {
	for !rf.killed() {
		rf.commitMu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.commitCond.Wait()
		}

		for rf.lastApplied < rf.commitIndex {
			applyIndex := rf.lastApplied + 1
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log.Entries[applyIndex].Command,
				CommandIndex: applyIndex,
			}
			rf.lastApplied++
		}

		rf.commitMu.Unlock()
	}
}

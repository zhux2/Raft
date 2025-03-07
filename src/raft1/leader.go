package raft

import (
	"math/rand"
	"sort"
	"time"
)

type LeaderState struct{}

func (leader *LeaderState) getState() ServerState {
	return ST_Leader
}

func (leader *LeaderState) tickerFunc(rf *Raft) {
	rf.sendHeartbeat()
	// pause for a random amount of time between 50 and 100
	// milliseconds.
	ms := 50 + (rand.Int63() % 100)
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// need to lock before call
func (leader *LeaderState) switchTo(rf *Raft) {
	rf.state = &rf.leaderState
	for i := range len(rf.peers) {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.log.LastIndex + 1
		rf.rollback[i] = 0
	}
}

// only for leader
func (rf *Raft) updateCommitIndex(r int) {
	if r < rf.commitIndex {
		return
	}
	rf.matchIndex[rf.me] = rf.log.LastIndex
	copySlice := append([]int(nil), rf.matchIndex...)
	sort.Ints(copySlice)
	newCommitIndex := max(rf.commitIndex, copySlice[rf.majority])
	if newCommitIndex > rf.commitIndex && rf.log.Entries[newCommitIndex].Term == rf.currentTerm {
		rf.commitMu.Lock()
		rf.commitIndex = newCommitIndex
		rf.commitCond.Signal()
		rf.commitMu.Unlock()
	}
}

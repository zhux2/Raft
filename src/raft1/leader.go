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
	ms := 50 + (rand.Int63() % 50)
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// need to lock before call
func (leader *LeaderState) switchTo(rf *Raft) {
	rf.state = &rf.leaderState
	for i := range len(rf.peers) {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.log.lastIndex + 1
		rf.rollback[i] = 0
	}
}

// only for leader
func (rf *Raft) updateCommitIndex(r int) {
	//l := rf.commitIndex
	//res := -1
	//for l <= r {
	//	mid := (l + r) / 2
	//	count := 0
	//	for i := 0; i < len(rf.peers); i++ {
	//		if i == rf.me {
	//			if rf.log.lastIndex >= mid {
	//				count++
	//			}
	//		} else {
	//			if rf.matchIndex[i] >= mid {
	//				count++
	//			}
	//		}
	//	}
	//	if count > rf.majority {
	//		l = mid + 1
	//		res = mid
	//	} else {
	//		r = mid - 1
	//	}
	//}
	//rf.commitIndex = max(rf.commitIndex, res)

	rf.matchIndex[rf.me] = rf.log.lastIndex
	copySlice := append([]int(nil), rf.matchIndex...)
	sort.Ints(copySlice)
	newCommitIndex := max(rf.commitIndex, copySlice[rf.majority])
	if newCommitIndex > rf.commitIndex {
		rf.commitMu.Lock()
		rf.commitIndex = newCommitIndex
		rf.commitCond.Signal()
		rf.commitMu.Unlock()
	}
}

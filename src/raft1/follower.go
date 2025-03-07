package raft

type FollowerState struct{}

func (follower *FollowerState) getState() ServerState {
	return ST_Follower
}

func (follower *FollowerState) tickerFunc(rf *Raft) {
	for rf.killed() == false {
		if rf.electionTimer.waitFor() {
			rf.mutex.Lock()
			rf.state = &rf.candidateState
			rf.dprint("switch to candidate")
			rf.mutex.Unlock()
			return
		}
	}
}

package raft

import (
	"context"
	"sync"
	"time"
)

type CandidateState struct {
	mutex    sync.Mutex
	voteGain int
}

func (candidate *CandidateState) getState() ServerState {
	return ST_Candidate
}

func (candidate *CandidateState) voteThread(rf *Raft, server int, args *RequestVoteArgs, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, &reply)
			if ok {
				rf.mutex.Lock()
				// "rf.state.getState() == ST_Candidate" this condition may be redundant
				if rf.state.getState() != ST_Candidate {
					rf.mutex.Unlock()
					return
				}
				if args.Term == rf.currentTerm && reply.VoteGranted {
					candidate.voteGain += 1
				}
				rf.mutex.Unlock()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// one tick is a round of vote
func (candidate *CandidateState) tickerFunc(rf *Raft) {
	rf.dprintf("Start Election")

	rf.mutex.Lock()
	if rf.state.getState() != ST_Candidate {
		rf.mutex.Unlock()
		return
	}
	rf.currentTerm += 1
	rf.votedFor = rf.me
	candidate.voteGain = 1
	args := RequestVoteArgs{
		Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.log.LastIndex, LastLogTerm: rf.log.LastTerm}
	rf.mutex.Unlock()
	rf.persist()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	for server := range len(rf.peers) {
		if server == rf.me {
			continue
		}
		wg.Add(1)
		go candidate.voteThread(rf, server, &args, ctx, &wg)
	}

	rf.electionTimer.resetTimer()

	for rf.killed() == false {
		rf.mutex.Lock()
		if rf.state.getState() == ST_Follower || rf.electionTimer.candidateTimeout() {
			rf.dprintf("Lose Election")
			rf.mutex.Unlock()
			break
		}
		if candidate.voteGain > rf.majority {
			rf.leaderState.switchTo(rf)
			rf.dprintf("Win Election")
			rf.sendHeartbeat()
			rf.mutex.Unlock()
			break
		}
		rf.mutex.Unlock()
		//time.Sleep(10 * time.Millisecond)
	}

	// clear all vote-thread
	go func() {
		cancel()
		wg.Wait()
	}()
}

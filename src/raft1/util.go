package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Tools

func expandSlice(slice []LogEntry, n int) []LogEntry {
	if n <= cap(slice) {
		return slice[:n]
	}

	newCap := ((n + 31) / 32) * 32

	newSlice := make([]LogEntry, n, newCap)

	copy(newSlice, slice)

	return newSlice[:n]
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) dprint(msg string) {
	//fmt.Printf("(%v) [%d, %s] - %s\n", time.Now(), rf.me, rf.stateName(), msg)
}

func (rf *Raft) dprintf(format string, a ...interface{}) {
	//fmt.Printf("(%v) [%d, %s] - %s\n", time.Now(), rf.me, rf.stateName(), fmt.Sprintf(format, a...))
}

func (rf *Raft) dprintf3C(format string, a ...interface{}) {
	//fmt.Printf("(%v) [%d, %s, %d] - %s\n", time.Now(), rf.me, rf.stateName(), rf.currentTerm, fmt.Sprintf(format, a...))
}

func (rf *Raft) dassert(cond bool, msg string, args ...interface{}) {
	if cond {
		return
	}
	fmt.Printf("assert fail [%d, %s] - ", rf.me, rf.stateName())
	fmt.Printf(msg, args...)
	fmt.Println()
}

const electionTimeoutL = 350
const electionTimeoutR = 550

func electionTimeout() time.Duration {
	randMillisecond := electionTimeoutL + rand.Intn(electionTimeoutR-electionTimeoutL)
	return time.Duration(randMillisecond) * time.Millisecond
}

// ----------------------------------------------------------------------------

type ElectionTimer struct {
	timer   *time.Timer
	mutex   sync.Mutex
	endTime time.Time
}

func makeElectionTimer() *ElectionTimer {
	electionTimer := new(ElectionTimer)
	duration := electionTimeout()
	electionTimer.endTime = time.Now().Add(duration)
	electionTimer.timer = time.NewTimer(duration)
	return electionTimer
}

func (et *ElectionTimer) resetTimer() {
	et.mutex.Lock()
	if !et.timer.Stop() {
		select {
		case <-et.timer.C:
		default:
		}
	}
	duration := electionTimeout()
	et.endTime = time.Now().Add(duration)
	et.timer.Reset(duration)
	et.mutex.Unlock()
}

func (et *ElectionTimer) waitFor() bool {
	for {
		et.mutex.Lock()
		select {
		case <-et.timer.C:
			et.mutex.Unlock()
			return true
		default:
			waitTime := et.endTime.Sub(time.Now())
			et.mutex.Unlock()
			time.Sleep(waitTime)
		}
	}
}

func (et *ElectionTimer) candidateTimeout() bool {
	et.mutex.Lock()
	defer et.mutex.Unlock()
	select {
	case <-et.timer.C:
		//rf.dprint("timer expired")
		return true
	default:
		//rf.dprint("no action")
		return false
	}
}

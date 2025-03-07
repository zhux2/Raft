package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Tools

func expandSlice(slice []LogEntry, n int) (bool, []LogEntry) {
	if n == len(slice) {
		return false, slice
	}
	if n <= cap(slice) {
		// 如果不需要扩容，仅调整长度，不影响原数据
		return true, slice[:n]
	}

	// 当 n > cap(slice) 时，计算新容量
	newCap := cap(slice) + ((n-cap(slice)+31)/32)*32

	// 分配新切片，长度为 n，容量为 newCap
	newSlice := make([]LogEntry, n, newCap)

	// 仅拷贝原始数据（新元素为零值）
	copy(newSlice, slice)

	return true, newSlice
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
	//electionTimer.resetChan = make(chan bool)
	duration := electionTimeout()
	electionTimer.endTime = time.Now().Add(duration)
	electionTimer.timer = time.NewTimer(duration)
	return electionTimer
}

func (et *ElectionTimer) resetTimer(rf *Raft) {
	//rf.dprint("reset timer")
	//select {
	//case et.resetChan <- true:
	//	rf.dprint("really reset timer")
	//default:
	//	rf.dprint("full reset timer")
	//}

	et.mutex.Lock()
	if !et.timer.Stop() {
		select {
		case <-et.timer.C:
		default:
		}
	}
	duration := electionTimeout()
	//rf.dprint(fmt.Sprintf("ElectionTimer reset %d", duration.Milliseconds()))
	et.endTime = time.Now().Add(duration)
	et.timer.Reset(duration)
	et.mutex.Unlock()
}

func (et *ElectionTimer) waitFor(rf *Raft) bool {
	for {
		et.mutex.Lock()
		select {
		case <-et.timer.C:
			//rf.dprint("timer expired")
			et.mutex.Unlock()
			return true
		default:
			waitTime := et.endTime.Sub(time.Now())
			et.mutex.Unlock()
			time.Sleep(waitTime)
			//case <-et.resetChan:
			//	rf.dprint("timer reset")
			//	et.electionTimerReset(rf)
			//	return false
		}
	}
}

func (et *ElectionTimer) candidateTimeout(rf *Raft) bool {
	et.mutex.Lock()
	defer et.mutex.Unlock()
	select {
	case <-et.timer.C:
		//rf.dprint("timer expired")
		return true
	//case <-et.resetChan:
	//	rf.dprint("timer reset")
	//	et.electionTimerReset(rf)
	//	return false
	default:
		//rf.dprint("no action")
		return false
	}
}

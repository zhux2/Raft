package raft

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}
type Log struct {
	LastIncludedIndex int
	LastIndex         int // true index
	LastTerm          int
	// Entries[0] contains entry with true index LastIncludedIndex
	Entries []LogEntry
}

func (log *Log) init() {
	log.Entries = make([]LogEntry, 0)
	log.Entries = append(log.Entries, LogEntry{Term: 0})
	log.LastIndex = 0
	log.LastTerm = 0
	log.LastIncludedIndex = 0
}

// convert true index to the array index of Log.Entries
func (log *Log) indexToArrid(index int) int {
	return index - log.LastIncludedIndex
}

func (log *Log) getEntry(index int) *LogEntry {
	return &log.Entries[log.indexToArrid(index)]
}

func (log *Log) getEntrySlice(startIndex int, endIndex int) []LogEntry {
	return log.Entries[log.indexToArrid(startIndex):log.indexToArrid(endIndex)]
}

// only need to set Command and Term of entry
func (log *Log) leaderAppend(entry LogEntry, rf *Raft) int {
	log.LastIndex += 1
	log.LastTerm = entry.Term
	entry.Index = log.LastIndex
	log.Entries = append(log.Entries, entry)
	return log.LastIndex
}

// return the true index
func (log *Log) followerAppend(ens []LogEntry, prevLogIndex int, rf *Raft) (bool, int) {
	if prevLogIndex+len(ens) > log.LastIndex {
		log.Entries = expandSlice(log.Entries, log.indexToArrid(prevLogIndex+len(ens)+1))
	}
	i := log.indexToArrid(prevLogIndex + 1)
	flag := false
	for j := 0; j < len(ens); j += 1 {
		// this happens if the entry with index i+j has been included in snapshot
		if i+j < 1 {
			continue
		}
		if flag {
			log.Entries[i+j] = ens[j]
		} else {
			if log.Entries[i+j].Term != ens[j].Term {
				log.Entries[i+j] = ens[j]
				flag = true
			}
		}
	}
	if flag {
		log.LastIndex = prevLogIndex + len(ens)
		log.LastTerm = log.getEntry(log.LastIndex).Term
		log.cutoffByLen(log.indexToArrid(log.LastIndex + 1))
	}
	return flag, prevLogIndex + len(ens)
}

func (log *Log) cutoffByLen(newLen int) {
	if newLen < len(log.Entries) {
		for i := newLen; i < len(log.Entries); i++ {
			log.Entries[i].Term = 0
		}
		log.Entries = log.Entries[:newLen]
	}
}

func (log *Log) discardBefore(index int) {
	log.Entries = log.Entries[log.indexToArrid(index):]
	log.LastIncludedIndex = index
}

func (log *Log) discardAll(lastIncludedIndex int, lastIncludedTerm int) {
	log.Entries = make([]LogEntry, 1)
	log.Entries[0] = LogEntry{
		Index:   lastIncludedIndex,
		Term:    lastIncludedTerm,
		Command: nil,
	}
	log.LastIndex = lastIncludedIndex
	log.LastTerm = lastIncludedTerm
	log.LastIncludedIndex = lastIncludedIndex
}

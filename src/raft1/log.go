package raft

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}
type Log struct {
	LastIndex int
	LastTerm  int
	Entries   []LogEntry
}

// only need to set Command and Term of entry
func (log *Log) leaderAppend(entry LogEntry, rf *Raft) int {
	log.LastIndex += 1
	log.LastTerm = entry.Term
	entry.Index = log.LastIndex
	log.Entries = append(log.Entries, entry)
	rf.dprintf3C("append log entry %v", entry)
	return log.LastIndex
}

func (log *Log) followerAppend(ens []LogEntry, prevLogIndex int, rf *Raft) (bool, int) {
	if prevLogIndex+len(ens) > log.LastIndex {
		log.Entries = expandSlice(log.Entries, prevLogIndex+len(ens)+1)
	}
	i := prevLogIndex + 1
	flag := false
	for j := 0; j < len(ens); j += 1 {
		if flag {
			rf.dprintf3C("append log entry %v", ens[j])
			log.Entries[i+j] = ens[j]
		} else {
			if log.Entries[i+j].Term != ens[j].Term {
				log.Entries[i+j] = ens[j]
				rf.dprintf3C("append log entry %v", ens[j])
				flag = true
			}
		}
	}
	if flag {
		log.LastIndex = prevLogIndex + len(ens)
		log.LastTerm = log.Entries[log.LastIndex].Term
		log.cutoff(log.LastIndex + 1)
	}
	return flag, prevLogIndex + len(ens)
}

func (log *Log) cutoff(newLen int) {
	if newLen < len(log.Entries) {
		for i := newLen; i < len(log.Entries); i++ {
			log.Entries[i].Term = 0
		}
		log.Entries = log.Entries[:newLen]
	}
}

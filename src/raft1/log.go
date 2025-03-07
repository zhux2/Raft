package raft

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}
type Log struct {
	lastIndex int
	lastTerm  int
	entries   []LogEntry
}

// only need to set Command and Term of entry
func (log *Log) leaderAppend(entry LogEntry) int {
	log.lastIndex += 1
	log.lastTerm = entry.Term
	entry.Index = log.lastIndex
	log.entries = append(log.entries, entry)
	return log.lastIndex
}

func (log *Log) followerAppend(ens []LogEntry, prevLogIndex int) int {
	log.entries = expandSlice(log.entries, prevLogIndex+len(ens)+1)
	i := prevLogIndex + 1
	flag := false
	for j := 0; j < len(ens); j += 1 {
		if flag {
			log.entries[i+j] = ens[j]
		} else {
			if log.entries[i+j].Term != ens[j].Term {
				log.entries[i+j] = ens[j]
				flag = true
			}
		}
	}
	//println("append length ", len(ens))
	log.lastIndex = prevLogIndex + len(ens)
	log.lastTerm = log.entries[log.lastIndex].Term
	return log.lastIndex
}

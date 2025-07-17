package raft

import "time"

type AppendEntriesArgs struct {
	Term         int        // current leader's term
	LeaderId     int        // current leader's id
	PrevLogIndex int        // index of leader's second last log
	PrevLogTerm  int        // term of leader's second last log
	Entries      []LogEntry // log entry need to be store, nil for heartbeat
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term     int  // term of server
	Success  bool // success or not
	Conflict bool // conflict or not
	XTerm    int  // term of conflicting entry
	XIndex   int  // index of first entry with XTerm in follower
	XLen     int  // length of follower's log
}

func (rf *Raft) appendLogs(args *AppendEntriesArgs) {
	DPrintf("%d start appending logs from leader %d", rf.me, args.LeaderId)
	for i := 0; i < len(args.Entries); i++ {
		logIndex := i + 1 + args.PrevLogIndex
		if logIndex >= len(rf.log) {
			// local log is shorter, just append the remaining log from args
			rf.log = append(rf.log, args.Entries[i:]...)
			return
		} else {
			if rf.log[logIndex].Term != args.Entries[i].Term {
				// if there's conflict, truncate and append the remaining
				rf.log = rf.log[:logIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				return
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%d received AppendEntries from %d, has %d entries", rf.me, args.LeaderId, len(args.Entries))
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Conflict = false
	reply.Success = false
	if args.Term < rf.currentTerm {
		DPrintf("%d reject AppendEntries from %d, reason: args term %d is less than current term %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	rf.resetElectionTimeouts()
	if args.Term > rf.currentTerm || (rf.state == Candidate && args.Term == rf.currentTerm) {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if len(rf.log) <= args.PrevLogIndex {
		// if follower's log is inconsistent with leader's, return conflict in reply
		// case 3: length of follower's log is less than prevLogIndex
		// set the nextIndex back to XLen
		reply.Conflict = true
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		xTerm := rf.log[args.PrevLogIndex].Term
		for index := args.PrevLogIndex; index > 0; index-- {
			if rf.log[index-1].Term != xTerm {
				reply.XIndex = index
				break
			}
		}
		reply.Conflict = true
		reply.XTerm = xTerm
		reply.XLen = len(rf.log)
		return
	}
	if args.Entries != nil {
		rf.appendLogs(args)
	}
	if rf.commitIndex < args.LeaderCommit {
		lastLogIndex := len(rf.log) - 1
		DPrintf("%d updating commitIndex %d, lastLogIndex %d, leader commitIndex %d", rf.me, rf.commitIndex, lastLogIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
		DPrintf("%d updated commitIndex to %d, starting apply", rf.me, rf.commitIndex)
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) buildArgs(server int) *AppendEntriesArgs {
	logIndex := rf.nextIndex[server] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: logIndex,
		PrevLogTerm:  rf.log[logIndex].Term,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	args.Entries = append(args.Entries, rf.log[rf.nextIndex[server]:]...)
	return args
}

// send log to each server, acquired lock after call Start()
func (rf *Raft) sendLogs(isHeartbeat bool) {
	if rf.state != Leader {
		return
	}
	DPrintf("%d send logs to server, term %d, isHeartbeat%v", rf.me, rf.currentTerm, isHeartbeat)
	for i := range rf.peers {
		if i == rf.me || (len(rf.log)-1 < rf.nextIndex[i] && !isHeartbeat) {
			continue
		}
		args := rf.buildArgs(i)
		go rf.sendAppendEntriesToServer(i, args)
	}
	rf.resetHeartBeatsTimeouts()
}

// called in different goroutine
func (rf *Raft) sendAppendEntriesToServer(server int, args *AppendEntriesArgs) {
	sendTime := time.Now().Format("2006/01/02 15:04:05.000000")
	reply := &AppendEntriesReply{}
	DPrintf("%d sending AppendEntries to %d, term %d, sendtime %v, length %d", rf.me, server, args.Term, sendTime, len(args.Entries))
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		DPrintf("%d did not receive AppendEntries reply from %d, term %d, sendtime %v", rf.me, server, args.Term, sendTime)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d received AppendEntries reply from %d, term %d, sendtime %v", rf.me, server, args.Term, sendTime)
	if rf.currentTerm != args.Term || rf.state != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if reply.Success {
		DPrintf("%d received success AppendEntries reply from %d", rf.me, server)
		match := args.PrevLogIndex + len(args.Entries)
		next := match + 1
		rf.nextIndex[server] = max(rf.nextIndex[server], next)
		rf.matchIndex[server] = max(rf.matchIndex[server], match)
		rf.leaderCommit()
	} else if reply.Conflict {
		// DPrintf("%d received conflict AppendEntries reply from %d, updating its nextIndex to %d", rf.me, server, rf.nextIndex[server])
		if reply.XTerm == -1 {
			//   Case 3: follower's log is too short: nextIndex = XLen
			rf.nextIndex[server] = reply.XLen
		} else {
			lastTermIndex := rf.findLastTermIndex(reply.XTerm)
			if lastTermIndex == -1 {
				// case1: leader didn't have XTerm nextIndex = XIndex
				rf.nextIndex[server] = reply.XIndex
			} else {
				// case2: leader did have XTerm
				// nextIndex = (index of leader's last entry for XTerm) + 1
				rf.nextIndex[server] = lastTermIndex + 1
			}
		}
		// newArgs := rf.buildArgs(server)
		// DPrintf("%d retry sending AppendEntries to %d", rf.me, server)
		// go rf.sendAppendEntriesToServer(server, newArgs)
	}
}

func (rf *Raft) findLastTermIndex(term int) int {
	for index := len(rf.log) - 1; index >= 0; index-- {
		if rf.log[index].Term == term {
			return index
		}
	}
	return -1
}

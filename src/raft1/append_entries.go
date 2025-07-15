package raft

import "time"

type Reason string

const (
	Conflict Reason = "Conflict"
)

type AppendEntriesArgs struct {
	Term         int        // current leader's term
	LeaderId     int        // current leader's id
	PrevLogIndex int        // index of leader's second last log
	PrevLogTerm  int        // term of leader's second last log
	Entries      []LogEntry // log entry need to be store, nil for heartbeat
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term         int    // term of server
	Success      bool   // success or not
	FailedReason Reason // reason for failure
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

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// if follower's log is inconsistent with leader's, return conflict in reply
		reply.FailedReason = Conflict
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

func (rf *Raft) buildArgs(server int, isHeatbeat bool) *AppendEntriesArgs {
	logIndex := rf.nextIndex[server] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: logIndex,
		PrevLogTerm:  rf.log[logIndex].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	if !isHeatbeat {
		args.Entries = make([]LogEntry, 0)
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[server]:]...)
	}
	return args
}

// send log to each server, acquired lock after call Start()
func (rf *Raft) sendLogs() {
	DPrintf("%d send logs to server, term %d", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me || len(rf.log)-1 < rf.nextIndex[i] {
			continue
		}
		args := rf.buildArgs(i, false)
		go rf.sendLogsToServer(i, args)
	}
}

// called in different goroutine
func (rf *Raft) sendLogsToServer(server int, args *AppendEntriesArgs) {
	sendTime := time.Now().Format("2006/01/02 15:04:05.000000")
	reply := &AppendEntriesReply{}
	DPrintf("%d sending logs to %d, term %d, sendtime %v, length %d", rf.me, server, args.Term, sendTime, len(args.Entries))
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		DPrintf("%d did not receive logs reply from %d, term %d, sendtime %v", rf.me, server, args.Term, sendTime)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d received logs reply from %d, term %d, sendtime %v", rf.me, server, args.Term, sendTime)
	if rf.currentTerm != args.Term || rf.state != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if reply.Success {
		DPrintf("%d received success logs reply from %d", rf.me, server)
		match := args.PrevLogIndex + len(args.Entries)
		next := match + 1
		rf.nextIndex[server] = max(rf.nextIndex[server], next)
		rf.matchIndex[server] = max(rf.matchIndex[server], match)
		rf.leaderCommit()
	} else if reply.FailedReason == Conflict {
		rf.nextIndex[server]--
		DPrintf("%d received conflict logs reply from %d, updating its nextIndex to %d", rf.me, server, rf.nextIndex[server])
		newArgs := rf.buildArgs(server, false)
		DPrintf("%d retry sending logs to %d", rf.me, server)
		go rf.sendLogsToServer(server, newArgs)
	}
}

// called after grab lock in ticker()
func (rf *Raft) sendHeartbeats() {
	if rf.state != Leader {
		return
	}
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	DPrintf("%d sending heartbeats, term %d", rf.me, term)
	for i := range rf.peers {
		if rf.me != i {
			go rf.sendHeartbeatToServer(i, term, commitIndex)
		}
	}
}

// sending heatbeat to target server
// called in different go routine
func (rf *Raft) sendHeartbeatToServer(server int, term int, commitIndex int) {
	sendTime := time.Now().Format("2006/01/02 15:04:05.000000")
	// DPrintf("%d sending heartbeats to %d, term %d, sendtime %v", rf.me, server, term, sendTime)
	//
	args := rf.buildArgs(server, true)
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		DPrintf("%d did not receive heartbeats from %d, term %d, sendtime %v", rf.me, server, term, sendTime)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("%d received reply with higher term, converting to Follower", rf.me)
		rf.becomeFollower(reply.Term)
	}
}

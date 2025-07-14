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
	Term    int  // term of server
	Success bool // success or not
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%d received AppendEntries from %d", rf.me, args.LeaderId)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimeouts()
	if args.Term > rf.currentTerm || (rf.state == Candidate && args.Term == rf.currentTerm) {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	if args.Entries != nil {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
	}

	if rf.commitIndex < args.LeaderCommit {
		lastLogIndex := len(rf.log) - 1
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendLogsToServer(args *AppendEntriesArgs) {
	// majority := len(rf.peers) / 2
	// finished := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		sendTime := time.Now().Format("2006/01/02 15:04:05.000000")
		go func(server int) {
			reply := &AppendEntriesReply{}
			DPrintf("%d sending heartbeats to %d, term %d, sendtime %v", rf.me, i, args.Term, sendTime)
			ok := rf.sendAppendEntries(server, args, reply)
			if ok {
				rf.mu.Lock()
				DPrintf("%d received logs reply from %d, term %d, sendtime %v", rf.me, server, args.Term, sendTime)
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				rf.mu.Unlock()
			} else {
				DPrintf("%d did not receive logs reply from %d, term %d, sendtime %v", rf.me, server, args.Term, sendTime)
			}
		}(i)
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

func (rf *Raft) sendHeartbeatToServer(server int, term int, commitIndex int) {
	sendTime := time.Now().Format("2006/01/02 15:04:05.000000")
	DPrintf("%d sending heartbeats to %d, term %d, sendtime %v", rf.me, server, term, sendTime)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: commitIndex,
	}
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		DPrintf("%d received heartbeats reply from %d, term %d, sendtime %v", rf.me, server, term, sendTime)
		if reply.Term > rf.currentTerm {
			DPrintf("%d received reply with higher term, converting to Follower", rf.me)
			rf.becomeFollower(reply.Term)
		}
		rf.mu.Unlock()
	} else {
		DPrintf("%d did not receive heartbeats from %d, term %d, sendtime %v", rf.me, server, term, sendTime)
	}
}

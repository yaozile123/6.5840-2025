package raft

import (
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // id of candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler, rf.me receive vote request from args.CandidateId
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d receiving request vote from %d, term %d, rf current state: term %d, vote for %d", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	if args.Term < rf.currentTerm {
		DPrintf("%d declined request vote from %d, term %d, reason: arg term less than current node's term %v < %v", rf.me, args.CandidateId, args.Term, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("%d declined request vote from %d, term %d, reason: already voted for %d", rf.me, args.CandidateId, args.Term, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		myLastLogTerm := rf.log[len(rf.log)-1].Term
		myLastLogIndex := len(rf.log) - 1
		DPrintf("%d declined request vote from %d, term %d, reason: candidate's log not up-to-date (candidate: term=%d, index=%d vs mine: term=%d, index=%d)",
			rf.me, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, myLastLogTerm, myLastLogIndex)
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	rf.resetElectionTimeouts()
	DPrintf("%d vote for %d with, term %d, vote result %v", rf.me, args.CandidateId, args.Term, reply.VoteGranted)
}

func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm > myLastLogTerm {
		return true
	}
	if lastLogTerm == myLastLogTerm && lastLogIndex >= len(rf.log)-1 {
		return true
	}
	return false
}

// should call after grab the lock under ticker()
func (rf *Raft) startElection() {
	if rf.state == Leader {
		return
	}
	rf.becomeCandidate()
	rf.resetElectionTimeouts()
	DPrintf("%d starting new election, term %d", rf.me, rf.currentTerm)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	total := len(rf.peers)
	majority := total/2 + 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVoteToServer(i, args, majority)
	}
}

func (rf *Raft) sendRequestVoteToServer(server int, args *RequestVoteArgs, majority int) {
	sendTime := time.Now().Format("2006/01/02 15:04:05.000000")
	reply := &RequestVoteReply{}
	DPrintf("%d sending vote request to %d, term: %d, sendtime %v", rf.me, server, args.Term, sendTime)
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		DPrintf("%d faild to receive vote reply from %d, sendtime %v", rf.me, server, sendTime)
		return
	}
	DPrintf("%d received vote reply from %d, result %v, sendtime %v", rf.me, server, reply.VoteGranted, sendTime)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != args.Term || rf.state != Candidate {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived >= majority {
			rf.becomeLeader()
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

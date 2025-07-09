package raft

import (
	"math/rand"
	"sync"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandiateId   int // id of candiate requesting vote
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
		}
		// reply.Term = args.Term
		reply.Term = rf.currentTerm
		if (rf.votedFor == -1 || rf.votedFor == args.CandiateId) && (rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex)) {
			rf.votedFor = args.CandiateId
			reply.VoteGranted = true
			rf.resetElectionTimeouts()
		} else {
			reply.VoteGranted = false
		}
	}
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

// called after grab lock in ticker()
func (rf *Raft) becomeLeader() {
	if rf.state == Leader {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.sendHeartbeats()

}

// should call after grab the lock under ticker()
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimeouts()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandiateId:   rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	currentTerm := rf.currentTerm
	count := 1
	finished := 1
	total := len(rf.peers)
	majority := total/2 + 1
	cond := sync.NewCond(&rf.mu)

	for i := range total {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			err := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if err {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
					rf.resetElectionTimeouts()
				} else if rf.state == Candidate && rf.currentTerm == currentTerm && reply.VoteGranted {
					count++
				}
			}
			finished++
			cond.Broadcast()
		}(i)
	}

	for count < majority && finished < total && rf.state == Candidate && rf.currentTerm == currentTerm {
		cond.Wait()
	}

	if count >= majority && rf.state == Candidate && rf.currentTerm == currentTerm {
		rf.becomeLeader()
	} else if rf.state == Candidate {
		rf.state = Follower
		extraDelay := time.Duration(rand.Intn(50)) * time.Millisecond
		rf.resetElectionTimeouts()
		rf.lastHeartbeat = time.Now().Add(extraDelay)
	}
}

func (rf *Raft) resetElectionTimeouts() {
	rf.lastHeartbeat = time.Now()
	rf.electionTimeouts = time.Duration(350+rand.Intn(200)) * time.Millisecond // 350-550ms
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

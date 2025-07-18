package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State string

const (
	Leader    State = "Leader"
	Follower  State = "Follower"
	Candidate State = "Candidate"
)

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *tester.Persister   // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill().
	currentTerm        int                 // current term
	votedFor           int                 // candidateId that received vote in current term
	commitIndex        int                 // index of highest log entry known to already be committed
	lastApplied        int                 // index of highest log entry already applied to state machine
	nextIndex          []int               // for each server, index of the next log entry to send to that server
	matchIndex         []int               // for each server, index of highest log entry known to be replicated on server
	log                []LogEntry          // index start at 1
	state              State               // candidate, leader, follower
	votesReceived      int
	electionTimeouts   time.Time
	heartbeatsTimeouts time.Time
	heartbeatsTime     time.Duration
	applyCond          *sync.Cond
	applyCh            chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	DPrintf("%d become to Follower from %v", rf.me, rf.state)
	rf.state = Follower
}

func (rf *Raft) becomeCandidate() {
	DPrintf("%d become to Candidate from %v", rf.me, rf.state)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1
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
	DPrintf("%d become leader, term %d", rf.me, rf.currentTerm)
	rf.sendLogs(true)
}

func (rf *Raft) resetElectionTimeouts() {
	timeout := time.Duration(175+rand.Intn(150)) * time.Millisecond
	rf.electionTimeouts = time.Now().Add(timeout)
}

func (rf *Raft) resetHeartBeatsTimeouts() {
	rf.heartbeatsTimeouts = time.Now().Add(rf.heartbeatsTime)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	termErr := d.Decode(&currentTerm)
	voteErr := d.Decode(&votedFor)
	logErr := d.Decode(&log)
	if termErr != nil || voteErr != nil || logErr != nil {
		DPrintf("termErr: %v, voteErr: %v, logErr: %v", termErr, voteErr, logErr)
		panic("error when read persist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) isElectionTimeout() bool {
	return time.Now().After(rf.electionTimeouts)
}

func (rf *Raft) isHeartbeatsTimeout() bool {
	return time.Now().After(rf.heartbeatsTimeouts)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, isLeader
	}
	DPrintf("leader %d term %d received command %v", rf.me, rf.currentTerm, command)
	isLeader = true
	term = rf.currentTerm
	index = len(rf.log)
	newLogEntry := LogEntry{Term: term, Command: command, Index: index}
	rf.log = append(rf.log, newLogEntry)
	DPrintf("leader current next index %v", rf.nextIndex)
	rf.sendLogs(false)
	return index, term, isLeader
}

// Update commitIndex and then send applyMsg, acquired lock in Start()
func (rf *Raft) leaderCommit() {
	if rf.state != Leader {
		return
	}
	DPrintf("leader %d committing logs", rf.me)
	for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
		DPrintf("checking log at index %d", i)
		if rf.log[i].Term != rf.currentTerm {
			continue
		}
		count := 1
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= i {
				count++
			}
		}
		// if most of the server replicated log[i], we can safe to commit
		if count > len(rf.peers)/2 {
			DPrintf("log at index %d become new commitIndex", i)
			rf.commitIndex = i
			rf.apply()
			return
		}
	}
}

// send apply msg after update the commitIndex
func (rf *Raft) apply() {
	DPrintf("server %d broadcast", rf.me)
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		DPrintf("%d applier wake up, current commitIndex %d, lastApplied %d", rf.me, rf.commitIndex, rf.lastApplied)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			DPrintf("server %d apply log at index %d", rf.me, rf.lastApplied)
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			// need tp unlock before send to channel
			// otherwise channel will block the thread untill msg get consumed
			rf.mu.Unlock()
			DPrintf("server %d sending command %v to applyCh", rf.me, applyMsg.Command)
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
		rf.applyCond.Wait()
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// DPrintf("checking status for node %d, state %v, term %d", rf.me, rf.state, rf.currentTerm)
		if rf.state == Leader {
			if rf.isHeartbeatsTimeout() {
				// DPrintf("leader %d heartbeats timeout, starting send heartbeats", rf.me)
				rf.sendLogs(true)
			} else {
				// DPrintf("%d heatbeat is not time out, remain: %v", rf.me, time.Until(rf.heartbeatsTimeouts))
			}
		} else {
			if rf.isElectionTimeout() {
				// DPrintf("%d election timeout, starting election", rf.me)
				rf.startElection()
			} else {
				// DPrintf("%d election is not timeout, remain: %v", rf.me, time.Until(rf.electionTimeouts))
			}
		}
		rf.mu.Unlock()
		// DPrintf("%d sleep for 15 ms", rf.me)
		time.Sleep(15 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Index: 0}
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.heartbeatsTime = time.Millisecond * 100
	rf.resetElectionTimeouts()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("server %d initialized", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}

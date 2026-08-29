package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Leader
	Candidate
)

type Log struct {
	Term      int
	Operation any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role Role

	currentTerm int
	votedFor    int
	logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastContact     time.Time
	electionTimeout time.Duration

	applyCh chan raftapi.ApplyMsg

	lastIncludeIndex int
	lastIncludeTerm  int

	snapshot []byte

	pendingSnapshot         []byte
	pendingLastIncludeIndex int
	pendingLastIncludeTerm  int
	isPendingSnapshot       bool

	applyCond   *sync.Cond
	replicateCh chan struct{}
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
	isleader = (rf.role == Leader)

	return term, isleader
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
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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

	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
		rf.commitIndex = rf.lastIncludeIndex
		rf.snapshot = data
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Log
	var currentTerm int
	var votedFor int
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&logs) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		return
	} else {
		rf.logs = logs
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludeIndex {
		return
	}

	rf.logs = append([]Log(nil), rf.logs[index-rf.lastIncludeIndex:]...)
	rf.lastIncludeTerm = rf.logs[0].Term
	rf.lastIncludeIndex = index

	rf.snapshot = snapshot

	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term        int
	CandidateID int

	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []Log

	LeaderCommit int
}

type AppendEntryReply struct {
	Term        int
	Success     bool
	BackupLen   int
	BackupTerm  int
	BackupIndex int
}

type InstallSnapshotArgs struct {
	Term             int
	Snapshot         []byte
	LastIncludeIndex int
	LastIncludeTerm  int
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower

		rf.persist()
	}

	if rf.votedFor == -1 && (args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
		(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term &&
			args.LastLogIndex >= rf.lastIncludeIndex+len(rf.logs)-1)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.lastContact = time.Now()

		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1

		rf.persist()
	}

	currentCommit := rf.commitIndex

	rf.lastContact = time.Now()
	rf.role = Follower

	reply.Term = rf.currentTerm
	reply.BackupTerm = -1
	reply.BackupIndex = -1
	if args.PrevLogIndex < rf.lastIncludeIndex {
		reply.BackupIndex = rf.lastIncludeIndex
		return
	}
	if args.PrevLogIndex >= 0 && (rf.lastIncludeIndex+len(rf.logs)-1 < args.PrevLogIndex ||
		rf.logs[args.PrevLogIndex-rf.lastIncludeIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		if rf.lastIncludeIndex+len(rf.logs)-1 < args.PrevLogIndex {
			reply.BackupLen = rf.lastIncludeIndex + len(rf.logs)
		} else if rf.logs[args.PrevLogIndex-rf.lastIncludeIndex].Term != args.PrevLogTerm {
			i := args.PrevLogIndex - rf.lastIncludeIndex
			for ; i > 0; i -= 1 {
				if rf.logs[i].Term != rf.logs[i-1].Term {
					break
				}
			}
			reply.BackupTerm = rf.logs[i].Term
			reply.BackupIndex = i + rf.lastIncludeIndex
		}
		return
	}

	i := args.PrevLogIndex + 1 - rf.lastIncludeIndex
	for ; i < len(rf.logs); i += 1 {
		if i-args.PrevLogIndex-1+rf.lastIncludeIndex >= len(args.Entries) ||
			rf.logs[i].Term != args.Entries[i-args.PrevLogIndex-1+rf.lastIncludeIndex].Term {
			break
		}
	}
	if i-args.PrevLogIndex-1+rf.lastIncludeIndex < len(args.Entries) {
		rf.logs = append(rf.logs[:i], args.Entries[i-args.PrevLogIndex-1+rf.lastIncludeIndex:]...)

		rf.persist()
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex && args.LeaderCommit < len(rf.logs)+rf.lastIncludeIndex {
		rf.commitIndex = args.LeaderCommit
	} else if args.LeaderCommit >= len(rf.logs)+rf.lastIncludeIndex {
		rf.commitIndex = len(rf.logs) - 1 + rf.lastIncludeIndex
	}

	if rf.commitIndex > currentCommit {
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1

		rf.persist()
	}

	rf.role = Follower
	rf.lastContact = time.Now()
	reply.Term = rf.currentTerm

	if args.LastIncludeIndex < rf.lastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	if args.LastIncludeIndex > rf.lastIncludeIndex+len(rf.logs)-1 {
		rf.snapshot = args.Snapshot
		rf.logs = append([]Log(nil), Log{Term: args.LastIncludeTerm})
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm
	} else if args.LastIncludeIndex <= rf.lastIncludeIndex+len(rf.logs)-1 {
		if rf.logs[args.LastIncludeIndex-rf.lastIncludeIndex].Term == args.LastIncludeTerm {
			rf.logs = append([]Log(nil), rf.logs[args.LastIncludeIndex-rf.lastIncludeIndex:]...)
			rf.lastIncludeIndex = args.LastIncludeIndex
			rf.lastIncludeTerm = args.LastIncludeTerm
			rf.snapshot = args.Snapshot
		} else {
			rf.logs = []Log{
				{Term: args.LastIncludeTerm},
			}
			rf.snapshot = args.Snapshot
			rf.lastIncludeIndex = args.LastIncludeIndex
			rf.lastIncludeTerm = args.LastIncludeTerm
		}
	}

	rf.persist()

	if rf.commitIndex < args.LastIncludeIndex {
		rf.commitIndex = args.LastIncludeIndex
	}

	if args.LastIncludeIndex > rf.lastApplied {
		rf.isPendingSnapshot = true
		rf.pendingLastIncludeIndex = args.LastIncludeIndex
		rf.pendingLastIncludeTerm = args.LastIncludeTerm
		rf.pendingSnapshot = args.Snapshot
	}

	rf.mu.Unlock()

	rf.applyCond.Broadcast()
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

func (rf *Raft) sendElection(server int, args *RequestVoteArgs, reply RequestVoteReply,
	ch chan *RequestVoteReply) {
	rf.sendRequestVote(server, args, &reply)
	ch <- &reply
}

func (rf *Raft) election() {
	rf.mu.Lock()
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm += 1
	rf.votedFor = rf.me

	rf.persist()

	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.CandidateID = rf.me
	args.Term = rf.currentTerm
	args.LastLogTerm = rf.logs[len(rf.logs)-1].Term

	args.LastLogIndex = len(rf.logs) - 1 + rf.lastIncludeIndex

	ch := make(chan *RequestVoteReply, len(rf.peers)-2)
	rf.mu.Unlock()

	votes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendElection(i, &args, reply, ch)
	}
	for i := 0; i < len(rf.peers)-1; i += 1 {
		reply := <-ch

		flag := false
		rf.mu.Lock()
		if rf.role != Candidate {
			flag = true
		}
		if reply.Term > rf.currentTerm {
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1

			rf.persist()

			flag = true
		} else if reply.Term == rf.currentTerm && reply.VoteGranted {
			votes += 1
		}

		if !flag && votes > len(rf.peers)/2 {
			rf.role = Leader
			for i := 0; i < len(rf.peers); i += 1 {
				rf.nextIndex[i] = len(rf.logs) + rf.lastIncludeIndex
			}
			for i := 0; i < len(rf.peers); i += 1 {
				rf.matchIndex[i] = rf.lastIncludeIndex
			}
			go rf.Contact()
			flag = true
		}
		rf.mu.Unlock()

		if flag {
			break
		}
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) startAppend(server int, args AppendEntryArgs) {
	ok := true
	for ok {
		rf.mu.Lock()
		reply := AppendEntryReply{}
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.LeaderCommit = rf.commitIndex
		if rf.nextIndex[server] <= rf.lastIncludeIndex {
			args := InstallSnapshotArgs{}
			args.LastIncludeIndex = rf.lastIncludeIndex
			args.LastIncludeTerm = rf.lastIncludeTerm
			args.Snapshot = rf.snapshot
			args.Term = rf.currentTerm
			reply := InstallSnapshotReply{}
			rf.mu.Unlock()
			rf.sendInstallSnapshot(server, args, &reply)

			rf.mu.Lock()
			if rf.role != Leader || rf.currentTerm > args.Term {
				rf.mu.Unlock()
				return
			}
			if reply.Term == rf.currentTerm && rf.nextIndex[server] <= args.LastIncludeIndex {
				rf.nextIndex[server] = args.LastIncludeIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1

				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			return
		}
		if rf.nextIndex[server] > 0 {
			args.PrevLogTerm = rf.logs[rf.nextIndex[server]-1-rf.lastIncludeIndex].Term
		}
		args.Entries = append([]Log(nil), rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if ok = rf.sendAppendEntry(server, &args, &reply); ok {
			rf.mu.Lock()
			if !reply.Success && reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1

				rf.persist()

				rf.mu.Unlock()
				return
			}
			if args.Term < rf.currentTerm || args.PrevLogIndex+1 < rf.nextIndex[server] {
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.mu.Unlock()
				rf.updateCommit()

				return
			} else {
				if reply.BackupTerm != -1 {
					rf.nextIndex[server] = reply.BackupIndex
				} else if reply.BackupIndex != -1 {
					rf.nextIndex[server] = reply.BackupIndex + 1
				} else {
					rf.nextIndex[server] = reply.BackupLen
				}
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentCommit := rf.commitIndex

	temp := make([]int, len(rf.matchIndex))
	copy(temp, rf.matchIndex)
	if rf.me < len(rf.peers)-1 {
		temp = append(temp[:rf.me], temp[rf.me+1:]...)
	} else {
		temp = temp[:rf.me]
	}

	sort.Ints(temp)
	if rf.logs[temp[len(rf.peers)-len(rf.peers)/2-1]-rf.lastIncludeIndex].Term == rf.currentTerm &&
		temp[len(rf.peers)-len(rf.peers)/2-1] > rf.commitIndex {
		rf.commitIndex = temp[len(rf.peers)-len(rf.peers)/2-1]
	}

	if rf.commitIndex > currentCommit {
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) Contact() {
	timer := time.NewTicker(time.Duration(100) * time.Millisecond)
	defer timer.Stop()

	args := AppendEntryArgs{}
	term, _ := rf.GetState()
	for {
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.LeaderCommit = rf.commitIndex
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.startAppend(i, args)
		}

		select {
		case <-rf.replicateCh:
		case <-timer.C:
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	log := Log{Operation: command, Term: rf.currentTerm}
	rf.logs = append(rf.logs, log)

	rf.persist()

	index = rf.lastIncludeIndex + len(rf.logs) - 1

	rf.mu.Unlock()

	select {
	case rf.replicateCh <- struct{}{}:
	default:
	}

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		isTimeout := (rf.electionTimeout < time.Since(rf.lastContact))
		if rf.role != Leader && isTimeout {
			rf.role = Candidate
			go rf.election()
			rf.electionTimeout = time.Duration((rand.Int63()%300)+500) * time.Millisecond
			rf.lastContact = time.Now()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex && !rf.isPendingSnapshot {
			rf.applyCond.Wait()
		}

		if !rf.isPendingSnapshot {
			lastApplied := rf.lastApplied
			commitIndex := rf.commitIndex

			if lastApplied < commitIndex {
				applyMsg := raftapi.ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.logs[lastApplied+1-rf.lastIncludeIndex].Operation
				applyMsg.CommandIndex = lastApplied + 1
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				rf.lastApplied += 1
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		} else {
			applyMsg := raftapi.ApplyMsg{}
			applyMsg.SnapshotValid = true
			applyMsg.SnapshotIndex = rf.pendingLastIncludeIndex
			applyMsg.SnapshotTerm = rf.pendingLastIncludeTerm
			applyMsg.Snapshot = rf.pendingSnapshot

			rf.isPendingSnapshot = false
			rf.mu.Unlock()

			rf.applyCh <- applyMsg

			rf.mu.Lock()
			rf.lastApplied = applyMsg.SnapshotIndex
			rf.mu.Unlock()
		}
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

	rf.votedFor = -1
	rf.lastContact = time.Now()
	rf.electionTimeout = time.Duration((rand.Int63()%300)+500) * time.Millisecond
	rf.logs = append(rf.logs, Log{})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	rf.isPendingSnapshot = false
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicateCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readPersist(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

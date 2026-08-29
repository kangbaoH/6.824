package rsm

import (
	"sync"

	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ID  int
	Me  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	nextID  int
	pending map[int]*Waiter
}

type Waiter struct {
	op Op
	ch chan any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		nextID:       0,
		pending:      make(map[int]*Waiter),
	}

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.Reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Reader() {
	for {
		msg := <-rsm.applyCh
		if msg.CommandValid {
			op := rsm.sm.DoOp(msg.Command.(Op).Req)

			rsm.mu.Lock()
			if rsm.maxraftstate != -1 && rsm.maxraftstate < rsm.rf.PersistBytes() {
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			}

			waiter, ok := rsm.pending[msg.CommandIndex]
			if !ok {
				rsm.mu.Unlock()
				continue
			}
			waiter.op = msg.Command.(Op)
			rsm.mu.Unlock()
			waiter.ch <- op

		} else if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Unlock()
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

	rsm.mu.Lock()
	op := Op{}
	op.ID = rsm.nextID
	rsm.nextID += 1
	op.Me = rsm.me
	op.Req = req

	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	waiter := Waiter{op: op, ch: make(chan any, 1)}
	rsm.pending[index] = &waiter
	rsm.mu.Unlock()

	for {
		select {
		case applyOp := <-waiter.ch:

			rsm.mu.Lock()
			if &waiter == rsm.pending[index] {
				delete(rsm.pending, index)
			}
			rsm.mu.Unlock()
			if waiter.op.Me == rsm.me && waiter.op.ID == op.ID {
				return rpc.OK, applyOp
			}

			return rpc.ErrWrongLeader, nil // i'm dead, try another server.

		case <-time.After(time.Duration(300 * time.Millisecond)):
			if currentTerm, isLeader := rsm.rf.GetState(); term != currentTerm || !isLeader {
				rsm.mu.Lock()
				if &waiter == rsm.pending[index] {
					delete(rsm.pending, index)
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

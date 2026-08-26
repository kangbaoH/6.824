package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.

	data map[string]ValueEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.

	kv.data = make(map[string]ValueEntry)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.data[args.Key]

	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = entry.Value
	reply.Version = entry.Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.data[args.Key]

	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		entry = ValueEntry{
			Value:   args.Value,
			Version: 1,
		}
		kv.data[args.Key] = entry
		reply.Err = rpc.OK
		return
	}

	if args.Version != entry.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	entry.Value = args.Value
	entry.Version += 1
	kv.data[args.Key] = entry

	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

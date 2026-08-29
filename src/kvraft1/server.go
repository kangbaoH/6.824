package kvraft

import (
	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValueEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.

	data map[string]ValueEntry
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here

	if args, ok := req.(rpc.GetArgs); ok {
		entry, ok := kv.data[args.Key]
		reply := rpc.GetReply{}
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		reply.Err = rpc.OK
		reply.Value = entry.Value
		reply.Version = entry.Version
		return reply
	} else if args, ok := req.(rpc.PutArgs); ok {
		entry, ok := kv.data[args.Key]
		reply := rpc.PutReply{}
		if !ok {
			if args.Version != 0 {
				reply.Err = rpc.ErrNoKey
				return reply
			}
			entry.Value = args.Value
			entry.Version = 1
			kv.data[args.Key] = entry
			reply.Err = rpc.OK
			return reply
		}

		if args.Version != entry.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}

		entry.Value = args.Value
		entry.Version += 1
		kv.data[args.Key] = entry
		reply.Err = rpc.OK

		return reply
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here

	if !(len(data) > 0) {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.data)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	err, result := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, result := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = result.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.data = make(map[string]ValueEntry)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}

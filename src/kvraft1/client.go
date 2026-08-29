package kvraft

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.

	leader := ck.leader

	args := rpc.GetArgs{}
	reply := rpc.GetReply{}
	args.Key = key
	for {
		reply = rpc.GetReply{}
		if ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply); !ok {
			leader = (leader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			leader = (leader + 1) % len(ck.servers)
		} else {
			ck.leader = leader
			break
		}
	}

	if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
		return reply.Value, reply.Version, reply.Err
	}
	return "", 0, ""
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	leader := ck.leader
	args := rpc.PutArgs{}
	reply := rpc.PutReply{}
	args.Key = key
	args.Value = value
	args.Version = version

	flag := false
	for {
		reply = rpc.PutReply{}
		if ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply); !ok {
			leader = (leader + 1) % len(ck.servers)
			flag = true
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			leader = (leader + 1) % len(ck.servers)
			flag = true
		} else if reply.Err == rpc.ErrVersion {
			ck.leader = leader
			if flag {
				reply.Err = rpc.ErrMaybe
			}
			break
		} else {
			ck.leader = leader
			break
		}
	}

	if reply.Err == rpc.OK || reply.Err == rpc.ErrMaybe || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrVersion {
		return reply.Err
	}
	return ""
}

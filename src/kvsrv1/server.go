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

type ValueTuple struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]ValueTuple
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{data: make(map[string]ValueTuple)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value, exists := kv.data[key]
	if exists {
		reply.Value = value.Value
		reply.Version = value.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value, exists := kv.data[key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
		} else {
			kv.data[key] = ValueTuple{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		}
	} else {
		if value.Version != args.Version {
			reply.Err = rpc.ErrVersion
		} else {
			kv.data[key] = ValueTuple{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}

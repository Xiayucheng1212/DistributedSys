package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvMap map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.kvMap[key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value

	kv.mu.Lock()
	kv.kvMap[key] = value
	kv.mu.Unlock()

	reply.Value = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value

	kv.mu.Lock()
	oldValue, ok := kv.kvMap[key];
	if ok {
		value = oldValue + value
	}
	kv.kvMap[key] = value
	kv.mu.Unlock()

	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)

	return kv
}

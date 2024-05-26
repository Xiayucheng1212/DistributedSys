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
	previousRequest map[int64]int64
	requestResult map[int64]string // ClerkId -> value
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if seqId, ok := kv.previousRequest[args.ClerkId]; ok {
		if seqId == args.SeqId {
			reply.Value = kv.requestResult[args.ClerkId]
			return
		}
	}
	kv.previousRequest[args.ClerkId] = args.SeqId

	if value, ok := kv.kvMap[key]; ok {
		reply.Value = value
		kv.requestResult[args.ClerkId] = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value

	kv.mu.Lock()
	if seqId, ok := kv.previousRequest[args.ClerkId]; ok {
		if seqId == args.SeqId {
			reply.Value = value
			kv.mu.Unlock()
			return
		}
	}

	kv.previousRequest[args.ClerkId] = args.SeqId

	kv.kvMap[key] = value
	kv.mu.Unlock()

	reply.Value = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value

	kv.mu.Lock()
	if seqId, ok := kv.previousRequest[args.ClerkId]; ok {
		if seqId == args.SeqId {
			reply.Value = kv.requestResult[args.ClerkId]
			kv.mu.Unlock()
			return
		}
	}
	kv.previousRequest[args.ClerkId] = args.SeqId

	oldValue, ok := kv.kvMap[key];
	if ok {
		value = oldValue + value
	} else {
		oldValue = ""
	}
	kv.kvMap[key] = value
	// request result returns old value
	kv.requestResult[args.ClerkId] = oldValue
	kv.mu.Unlock()

	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.previousRequest = make(map[int64]int64)
	kv.requestResult = make(map[int64]string)

	return kv
}

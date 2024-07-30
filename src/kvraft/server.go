package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opr string
	Key string
	Value string
	ClerkId int64
	SeqId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv map[string]string // key-value store
	ack map[int64]int64 // record the latest sequence number of each client
	waitChs  map[int]chan Op   // channels to notify waiting RPC handlers
	lastApplied int            // last applied Raft log index
}

func (kv *KVServer) operationReader() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}
		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)
			if lastSeq, ok := kv.ack[op.ClerkId]; !ok || op.SeqId > lastSeq {
				switch op.Opr {
				case "Put":
					kv.kv[op.Key] = op.Value
				case "Append":
					kv.kv[op.Key] += op.Value
				}
				kv.ack[op.ClerkId] = op.SeqId
			}
			if ch, ok := kv.waitChs[msg.CommandIndex]; ok {
				ch <- op
			}
			kv.lastApplied = msg.CommandIndex
			kv.mu.Unlock()
		}
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if seq, ok := kv.ack[args.ClerkId]; ok && seq >= args.SeqId {
		reply.Err = OK
		reply.Value = kv.kv[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Opr: "Get", Key: args.Key, ClerkId: args.ClerkId, SeqId: args.SeqId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(index)
	defer kv.removeWaitCh(index)

	select {
	case appliedOp := <-ch:
		if appliedOp.ClerkId == args.ClerkId && appliedOp.SeqId == args.SeqId {
			kv.mu.Lock()
			reply.Value = kv.kv[args.Key]
			reply.Err = OK
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	// Notice: timeout when there is a deadlock, release the lock and do it again
	case <-time.After(2 * time.Second): 
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Opr: "Put", Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, SeqId: args.SeqId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(index)
	defer kv.removeWaitCh(index)

	select {
	case appliedOp := <-ch:
		if appliedOp.ClerkId == args.ClerkId && appliedOp.SeqId == args.SeqId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(2 * time.Second):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Opr: "Append", Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, SeqId: args.SeqId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(index)
	defer kv.removeWaitCh(index)

	select {
	case appliedOp := <-ch:
		if appliedOp.ClerkId == args.ClerkId && appliedOp.SeqId == args.SeqId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(2 * time.Second):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.waitChs[index]; !ok {
		kv.waitChs[index] = make(chan Op, 1)
	}
	return kv.waitChs[index]
}

func (kv *KVServer) removeWaitCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.waitChs, index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.waitChs = make(map[int]chan Op)
	kv.lastApplied = 0

	go kv.operationReader()

	return kv
}

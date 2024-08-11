package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "sync/atomic"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Id int64
	SeqId int64
	PrevLeader int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.Id = nrand() % 100
	ck.SeqId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// Notice: need use atomic operation to update SeqId and PrevLeader
	SeqId := atomic.AddInt64(&ck.SeqId, 1)
	PrevLeader := atomic.LoadInt32(&ck.PrevLeader)

	args := GetArgs{Key: key, ClerkId: ck.Id, SeqId: SeqId}
	reply := GetReply{}
	// send an RPC request to a random server, and keep trying indefinitely until find the Raft leader
	for i := int(PrevLeader); ; i = (i + 1) % len(ck.servers) {
		fmt.Printf("Get %v %v, %v\n", args, reply, i)
 		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if (ok && reply.Err == OK) {
			atomic.StoreInt32(&ck.PrevLeader, int32(i))
			break
		}
	}

	// update PrevLeader
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// Notice: need use atomic operation to update SeqId and PrevLeader
	SeqId := atomic.AddInt64(&ck.SeqId, 1)
	PrevLeader := atomic.LoadInt32(&ck.PrevLeader)

	args := PutAppendArgs{Key: key, Value: value, ClerkId: ck.Id, SeqId: SeqId, Op: op}
	for i := int(PrevLeader); ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		ok := false
		ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)


		if (ok && reply.Err == OK) {
			atomic.StoreInt32(&ck.PrevLeader, int32(i))
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

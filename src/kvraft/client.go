package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Id int64
	SeqId int64
	PrevLeader int
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
	args := GetArgs{Key: key, ClerkId: ck.Id, SeqId: ck.SeqId}
	reply := GetReply{}
	// send an RPC request to a random server, and keep trying indefinitely until find the Raft leader
	for i := ck.PrevLeader; ; i = (i + 1) % len(ck.servers) {
		fmt.Printf("Get %v %v, %v\n", args, reply, i)
 		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if (ok && reply.Err == OK) {
			ck.SeqId++
			ck.PrevLeader = i
			break
		}
	}
	return ""
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
	args := PutAppendArgs{Key: key, Value: value, ClerkId: ck.Id, SeqId: ck.SeqId}
	for i := ck.PrevLeader; ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		ok := false
		if op == "Put" {
			fmt.Printf("Put %v %v, %v\n", args, reply, i)
			ok = ck.servers[i].Call("KVServer.Put", &args, &reply)
		} else {
			fmt.Printf("Append %v %v, %v\n", args, reply, i)
			ok = ck.servers[i].Call("KVServer.Append", &args, &reply)
		}

		// TODO: wait for the leader to commit the request
		if (ok && reply.Err == OK) {
			ck.SeqId++
			ck.PrevLeader = i
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

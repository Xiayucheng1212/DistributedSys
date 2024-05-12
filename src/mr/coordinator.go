package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Coordinator struct {
	// Your definitions here.
	fileNames []string
	nReduce int
	nextIndex int // next file index to assign to worker
	nextReduceIndex int // next reduce index to assign to worker
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	// Your code here.
	// lock the coordinator
	reply.NReduce = c.nReduce
	reply.NMap = len(c.fileNames)
	if c.nextIndex >= len(c.fileNames) && c.nextReduceIndex < c.nReduce {
		// assign reduce task
		reply.FileName = ""
		reply.TaskType = "reduce"
		c.mu.Lock()
		reply.TaskIndex = c.nextReduceIndex
		c.nextReduceIndex++
		c.mu.Unlock()
	} else if c.nextIndex < len(c.fileNames) {
		// assign map task
		reply.FileName = c.fileNames[c.nextIndex]
		reply.TaskType = "map"
		c.mu.Lock()
		reply.TaskIndex = c.nextIndex
		c.nextIndex++
		c.mu.Unlock()
	} else {
		reply.TaskType = "no more tasks"
	}
	
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.nextIndex >= len(c.fileNames) && c.nextReduceIndex >= c.nReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.fileNames = files
	c.nReduce = nReduce
	c.nextIndex = 0
	c.nextReduceIndex = 0
	c.mu = sync.Mutex{}

	c.server()
	return &c
}

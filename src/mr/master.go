package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	idle int = iota
	inProgress
	completed
)

type Master struct {
	// Your definitions here.
	mu sync.Mutex
	R int
	M int
	MapMonitor []*MapInfo
	ReduceMonitor []*ReduceInfo
}

type MapInfo struct {
	Filename string
	State int
	Done chan struct{}
}

type ReduceInfo struct {
	State int
	Done chan struct{}
}

func countDownMapTask(m *Master, mi *MapInfo) {
	select {
	case <-time.After(10 * time.Second):
		m.mu.Lock()
		if mi.State == inProgress {
			mi.State = idle
		}
		m.mu.Unlock()
		return
	case <-mi.Done:
		return
	}
}

func countDownReduceTask(m *Master, ri *ReduceInfo) {
	select {
	case <-time.After(10 * time.Second):
		m.mu.Lock()
		if ri.State == inProgress {
			ri.State = idle
		}
		m.mu.Unlock()
		return
	case <-ri.Done:
		return
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	// Assign map task and check if all map tasks are completed
	for completedMapTask := 0; completedMapTask < len(m.MapMonitor); {
		m.mu.Lock()
		for idx, mi := range m.MapMonitor {
			if mi.State == completed {
				completedMapTask ++
			} else {
				completedMapTask = 0
			}
			if mi.State == idle {
				reply.TaskType = MapTask
				reply.R = m.R
				reply.Filename = mi.Filename
				reply.ID = idx
				mi.State = inProgress
				miHandle := mi
				go countDownMapTask(m, miHandle)
				m.mu.Unlock()
				return nil
			}
		}
		m.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
	// Assign reduce task
	m.mu.Lock()
	for idx, ri := range m.ReduceMonitor {
		if ri.State == idle {
			reply.TaskType = ReduceTask
			reply.M = m.M
			reply.ID = idx
			ri.State = inProgress
			riHandle := ri
			go countDownReduceTask(m, riHandle)
			m.mu.Unlock()
			return nil
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := args.ID
	if args.TaskType == MapTask {
		if 0 <= i && i < m.M && m.MapMonitor[i].State == inProgress {
			m.MapMonitor[i].State = completed
			m.MapMonitor[i].Done <- struct{}{}
			return nil
		}
	} else { // args.TaskType == ReduceTask
		if 0 <= i && i < m.R && m.ReduceMonitor[i].State == inProgress {
			m.ReduceMonitor[i].State = completed
			m.ReduceMonitor[i].Done <- struct{}{}
			return nil
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rm := range m.ReduceMonitor {
		if rm.State != completed {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		R:             nReduce,
		M:             len(files),
		MapMonitor:    make([]*MapInfo, 0, len(files)),
		ReduceMonitor: make([]*ReduceInfo, 0, nReduce),
	}
	for _, filename := range files {
		m.MapMonitor = append(m.MapMonitor, &MapInfo{
			Filename: filename,
			State:    idle,
			Done:     make(chan struct{}),
		})
	}
	for i := 0; i < nReduce; i ++ {
		m.ReduceMonitor = append(m.ReduceMonitor, &ReduceInfo{
			State: idle,
			Done:  make(chan struct{}),
		})
	}
	m.server()
	return &m
}

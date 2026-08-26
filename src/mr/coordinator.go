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

type TaskStat int

const (
	Idle TaskStat = iota
	Processing
	Finish
)

type Phase int

const (
	Map Phase = iota
	Reduce
	Done
)

type Coordinator struct {
	// Your definitions here.

	tasks   []Task
	nMap    int
	nReduce int
	phase   Phase
	mutex   sync.Mutex
}

type Task struct {
	TaskID    int
	TaskType  TaskType
	TaskStat  TaskStat
	Filename  string
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch c.phase {
	case Map:

		for i := range c.tasks {
			if c.tasks[i].TaskStat == Idle || (c.tasks[i].TaskStat == Processing &&
				time.Since(c.tasks[i].StartTime).Seconds() > 10) {

				c.tasks[i].StartTime = time.Now()
				c.tasks[i].TaskStat = Processing

				reply.TaskID = c.tasks[i].TaskID
				reply.Filename = c.tasks[i].Filename
				reply.TaskType = MapTask
				reply.NReduce = c.nReduce

				return nil
			}
		}
		reply.TaskType = WaitTask

	case Reduce:

		for i := range c.tasks {
			if c.tasks[i].TaskStat == Idle || (c.tasks[i].TaskStat == Processing &&
				time.Since(c.tasks[i].StartTime).Seconds() > 10) {

				c.tasks[i].StartTime = time.Now()
				c.tasks[i].TaskStat = Processing

				reply.TaskID = c.tasks[i].TaskID
				reply.TaskType = ReduceTask
				reply.NMap = c.nMap

				return nil
			}
		}
		reply.TaskType = WaitTask

	case Done:
		reply.TaskType = ExitTask
	}

	return nil
}

func (c *Coordinator) FinishTask(args *TaskFinishArgs, reply *TaskFinishReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if int(args.TaskType) == int(c.phase) {
		c.tasks[args.TaskID].TaskStat = Finish
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch c.phase {
	case Map:

		flag := true
		for _, task := range c.tasks {
			if task.TaskStat != Finish {
				flag = false
			}
		}
		if flag {
			c.phase = Reduce
			c.tasks = nil

			task := Task{}
			for i := 0; i < c.nReduce; i += 1 {
				task.TaskID = i
				task.TaskStat = Idle
				task.TaskType = ReduceTask

				c.tasks = append(c.tasks, task)
			}
		}

	case Reduce:

		flag := true
		for _, task := range c.tasks {
			if task.TaskStat != Finish {
				flag = false
			}
		}
		if flag {
			c.phase = Done
			ret = true
		}
	case Done:
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{} //初始化coordinator，接收参数sockname，文件列表和reduce task数量

	// Your code here.

	c.nMap = len(files)
	for i := 0; i < c.nMap; i += 1 {
		task := Task{}
		task.TaskID = i
		task.TaskType = MapTask
		task.TaskStat = Idle
		task.Filename = files[i]

		c.tasks = append(c.tasks, task)
	}

	c.nReduce = nReduce
	c.phase = Map

	c.server(sockname)
	return &c
}

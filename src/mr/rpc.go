package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskRequestArgs struct {
}

type TaskRequestReply struct {
	TaskID   int
	Filename string
	TaskType TaskType
	NMap     int
	NReduce  int
}

type TaskFinishArgs struct {
	TaskID   int
	TaskType TaskType
}

type TaskFinishReply struct {
}

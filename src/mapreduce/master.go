package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// fetch a worker for map/reduce task
func (mr *MapReduce) FetchWorker(workersChannel chan string) string {
	worker := <-workersChannel
	mr.mutex.Lock()
	mr.Workers[worker] = &WorkerInfo{worker}
	mr.mutex.Unlock()
	return worker
}

// send RPC to rpc name handler
func (mr *MapReduce) SendRPC(worker string, args *DoJobArgs, workersChannel chan string, jobType bool) {
	var reply DoJobReply
	var job string
	switch jobType {
	case true:
		job = "Map"
	case false:
		job = "Reduce"
	}
	success := call(worker, "Worker.DoJob", args, &reply)
	if !success {
		fmt.Printf("Worker failed in %s job, reassigning...\n", job)
		new_worker := mr.FetchWorker(workersChannel)
		mr.SendRPC(new_worker, args, workersChannel, jobType)
	} else {
		mr.registerChannel <- worker
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Run Map tasks first
	for i := 0; i < mr.nMap; i++ {
		jobNumber := i
		worker := mr.FetchWorker(mr.registerChannel)
		args := &DoJobArgs{mr.file, Map, jobNumber, mr.nReduce}
		go mr.SendRPC(worker, args, mr.registerChannel, true)
	}

	// Run Reduce tasks
	for i := 0; i < mr.nReduce; i++ {
		jobNumber := i
		worker := mr.FetchWorker(mr.registerChannel)
		args := &DoJobArgs{mr.file, Reduce, jobNumber, mr.nMap}
		go mr.SendRPC(worker, args, mr.registerChannel, false)
	}

	return mr.KillWorkers()
}

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

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	nWk := 2
	wks := make([]string, nWk)
	for i := 0; i < nWk; i++ {
		e := <-mr.registerChannel
		// mr.Workers[e] = &WorkerInfo{e}
		wks[i] = e
	}
	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
		var reply DoJobReply
		ok := (call(wks[i%nWk], "Worker.DoJob", args, &reply) || call(wks[i%nWk^1], "Worker.DoJob", args, &reply))
		for !ok {
			fmt.Printf("DoJob Map RPC %d error,all failed\n", i)
			wks[0] = <-mr.registerChannel
			wks[1] = <-mr.registerChannel
			ok = (call(wks[i%nWk], "Worker.DoJob", args, &reply) || call(wks[i%nWk^1], "Worker.DoJob", args, &reply))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
		var reply DoJobReply
		ok := (call(wks[i%nWk], "Worker.DoJob", args, &reply) || call(wks[i%nWk^1], "Worker.DoJob", args, &reply))
		for !ok {
			fmt.Printf("DoJob Map RPC %d error,all failed\n", i)
			wks[0] = <-mr.registerChannel
			wks[1] = <-mr.registerChannel
			ok = (call(wks[i%nWk], "Worker.DoJob", args, &reply) || call(wks[i%nWk^1], "Worker.DoJob", args, &reply))
		}
	}
	return mr.KillWorkers()
}

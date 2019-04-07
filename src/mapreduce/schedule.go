package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	workers :=[]string{}
	i := 0
	for{
		worker:= <- registerChan
		workers = append(workers,worker)
		i++
		if i >= 2{
			break
		}
	}	
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		for index,file:= range mapFiles{
			args := &DoTaskArgs{jobName,file,phase,index,n_other}
			fmt.Println(args)
			worker := workers[0]
			if index > 15{
				worker = workers[1]
			}
			call(worker, "Worker.DoTask",args,nil)
		}
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		for i := 0; i < nReduce;i++{
			args := &DoTaskArgs{jobName,"",phase,i,n_other}
			worker := workers[0]
			if i > 5{
				worker = workers[1]
			}
			call(worker, "Worker.DoTask",args,nil)
		}
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}

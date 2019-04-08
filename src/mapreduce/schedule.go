package mapreduce

import "fmt"
import "sync"
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
	var wg sync.WaitGroup
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		for index,file:= range mapFiles{
			args := &DoTaskArgs{jobName,file,phase,index,n_other}
			fmt.Println(args)
			wg.Add(1)
			go func(args *DoTaskArgs){
				worker := <- registerChan
				call(worker, "Worker.DoTask",args,nil)				
				registerChan <- worker
				wg.Done()
			}(args)					
		}
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		for i := 0; i < nReduce;i++{
			args := &DoTaskArgs{jobName,"",phase,i,n_other}
			wg.Add(1)
			go func(args *DoTaskArgs){
				worker := <- registerChan
				call(worker, "Worker.DoTask",args,nil)				
				wg.Done()
				registerChan <- worker
			}(args)					
		}
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

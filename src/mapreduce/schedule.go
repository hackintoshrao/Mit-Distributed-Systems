package mapreduce

import "fmt"
import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	// used to wait for workers to finish the task.
	var wg sync.WaitGroup

	// iterate through total number of tasks.
	for i := 1; i <= ntasks; i++ {
		wg.Add(1)
		// the workers are registered in the channel below, pick the available workers from the channel below.
		w := <-mr.registerChannel
		// Request the workers to do the task concurrently using go routine.
		go func(i int, mr *Master, phase jobPhase, nios int, w string) {
			// arguments for the RPC call.
			args := &DoTaskArgs{
				JobName:       mr.jobName,
				File:          mr.files[i-1],
				TaskNumber:    i - 1,
				Phase:         phase,
				NumOtherPhase: nios,
			}
			// execute the RPC call.
			_ = call(w, "Worker.DoTask", args, new(struct{}))
			mr.Lock()
			defer mr.Unlock()
			debug("Register: worker after RPC  %s\n", w)
			// confirm that the response has arrived.
			wg.Done()
			// the worker is free now, add it to the list of available workers.
			mr.workers = append(mr.workers, w)
			go func() {
				// Add the worker back to the channel of avaialble workers/
				mr.registerChannel <- w
			}()

		}(i, mr, phase, nios, w)
	}
	// wait till all the workers finish the task.
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

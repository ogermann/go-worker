package worker

// WorkerQueue is a simple queue maintaining it's order. A preconfigured worker count determines how
// many worker executing jobs in the queue.
// If there's more than one worker configured, jobs will be executed in parallel, so be sure these jobs
// only use external data that can be accessed in parallel.
type WorkerQueue interface {
	// Do register a func for being processed by a worker when the worker is ready
	Do(f func())
	// Close the jobs channel so no new jobs can be queued. It is not necessary to close the channel
	Close()
	// GetQueueSize return the size of all waiting jobs in the queue
	GetQueueSize() int
}

type workerPool struct {
	workerCount int
	jobs        chan func()
}

// NewWorkerQueue spawns a new set of worker that accepts jobs which return void to be executed in parallel.
// after creation use the Do func to register jobs for processing.
func NewWorkerQueue(workerCount int, bufferSize int) WorkerQueue {
	w := &workerPool{
		workerCount: workerCount,
	}

	w.jobs = make(chan func(), bufferSize)

	for i := 0; i < w.workerCount; i++ {
		go worker(w.jobs)
	}

	return w
}

func worker(jobs chan func()) {
	for j := range jobs {
		j()
	}
}

func (w *workerPool) Do(f func()) {
	w.jobs <- f
}

func (w *workerPool) Close() {
	close(w.jobs)
}

func (w *workerPool) GetQueueSize() int {
	return len(w.jobs)
}

package worker

import (
	"context"
	"github.com/ogermann/go-hashset"
	"sync"
)

// DistinctWorkerQueue is a queue taking DistinctiveJob(s) and executing their Run methods when they
// are ready. The execution order is not guaranteed. A new job won't be added if there already is another
// equal job with the same id is in the queue.
// If there's more than one worker configured, jobs will be executed in parallel, so be sure these jobs
// only use external data that can be accessed in parallel.
type DistinctWorkerQueue interface {
	// Do register a DistinctiveJob for being processed by a worker when the worker is ready, the
	// job will be ignored if there's already an equal unprocessed job in the queue
	Do(job DistinctiveJob)
	// Close the jobs channel so no new jobs can be queued. It is not necessary to close the channel
	Close()
	// GetQueueSize return the size of all waiting jobs in the queue
	GetQueueSize() int
}

type Job interface {
	Run(ctx context.Context)
}

type DistinctiveJob interface {
	Job
	hashSet.Comparable
}

const workerId = "workerId"

type distinctWorkerQueue struct {
	sync.Mutex
	index  hashSet.HashSet
	queue  chan DistinctiveJob
	cancel context.CancelFunc
}

func NewDistinctWorkerQueue(workerCount int, bufferSize int) *distinctWorkerQueue {
	instance := &distinctWorkerQueue{
		index: hashSet.NewHashSet(),
		queue: make(chan DistinctiveJob, bufferSize),
	}

	var ctx context.Context
	ctx, instance.cancel = context.WithCancel(context.Background())

	for i := 0; i < workerCount; i++ {
		_ctx := context.WithValue(ctx, workerId, i)
		go instance.worker(_ctx)
	}

	return instance
}

func (q *distinctWorkerQueue) Do(job DistinctiveJob) {
	err := q.index.Add(job)
	if err == nil {
		q.queue <- job
	}
}

func (q *distinctWorkerQueue) worker(ctx context.Context) {
	for job := range q.queue {
		_ = q.index.Remove(job)
		job.Run(ctx)
	}
}

func (q *distinctWorkerQueue) Close() {
	close(q.queue)
	q.cancel()
}

func (q *distinctWorkerQueue) GetQueueSize() int {
	return len(q.queue)
}

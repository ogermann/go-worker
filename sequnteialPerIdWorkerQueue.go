package worker

import (
	"context"
	"sync"
)

type SequentialPerIdJob interface {
	Job
	Id() string
}

// SequentialPerIdWorkerQueue is a worker queue, that guarantees a single threaded execution for all jobs with the
// same comparable result.
type SequentialPerIdWorkerQueue interface {
	// Do register a DistinctiveJob for being processed by a worker when the worker is ready, the
	// job will be scheduled after other jobs with same id and will never run in parallel with them.
	Do(job SequentialPerIdJob)
	// Close the jobs channel so no new jobs can be queued. It is not necessary to close the channel for cleanup.
	Close()
	// GetQueueSize return the size of all waiting jobs in the queue
	GetQueueSize() int
}

type sequentialPerIdWorkerQueue struct {
	sync.RWMutex
	sequentialQueue           map[string]chan SequentialPerIdJob
	sequentialQueueBufferSize int
	queue                     chan SequentialPerIdJob
	cancel                    context.CancelFunc
}

func NewSequentialPerIdWorkerQueue(ctx context.Context, workerCount int, bufferSize int, sequentialQueueBufferSize int) SequentialPerIdWorkerQueue {
	instance := &sequentialPerIdWorkerQueue{
		sequentialQueue:           make(map[string]chan SequentialPerIdJob),
		queue:                     make(chan SequentialPerIdJob, bufferSize),
		sequentialQueueBufferSize: sequentialQueueBufferSize,
	}

	var innerCtx context.Context
	innerCtx, instance.cancel = context.WithCancel(ctx)

	for i := 0; i < workerCount; i++ {
		_ctx := context.WithValue(innerCtx, workerId, i)
		go instance.worker(_ctx)
	}

	return instance
}

func (q *sequentialPerIdWorkerQueue) Do(job SequentialPerIdJob) {
	q.queue <- job
}

func (q *sequentialPerIdWorkerQueue) registerJob(ctx context.Context, originJob SequentialPerIdJob) {
	// check if there are jobs in the sequentialQueue to process first
	q.Lock()
	sq, ok := q.sequentialQueue[originJob.Id()]
	if !ok {
		sq = make(chan SequentialPerIdJob, q.sequentialQueueBufferSize)
		q.Unlock()
		select {
		case sq <- originJob:
			break
		case <-ctx.Done():
			return
		}
	} else {
		// add the new job to the sequential queue and let it being processed by the worker that created the cahnnel
		defer q.Unlock()
		select {
		case sq <- originJob:
			return
		case <-ctx.Done():
			return
		}
	}

	// work the sequential queue
	for job := range sq {
		if ctx.Err() != nil {
			return
		}

		job.Run(ctx)

		if len(sq) == 0 {
			q.Lock()
			if len(sq) > 0 {
				q.Unlock()
				continue
			}
			delete(q.sequentialQueue, job.Id())
			q.Unlock()
			break
		}
	}
}

func (q *sequentialPerIdWorkerQueue) worker(ctx context.Context) {
	for job := range q.queue {
		q.registerJob(ctx, job)
	}
}

func (q *sequentialPerIdWorkerQueue) Close() {
	close(q.queue)
	q.cancel()
}

func (q *sequentialPerIdWorkerQueue) GetQueueSize() int {
	l := len(q.queue)
	q.RLock()
	defer q.RUnlock()
	for _, sq := range q.sequentialQueue {
		l += len(sq)
	}
	return l
}

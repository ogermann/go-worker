package worker

import (
	"context"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

type testJob struct {
	id        uint64
	controlCh chan uint64
	job       func()
}

func (j *testJob) Id() string {
	return strconv.Itoa(int(j.id))
}

func (j *testJob) Run(ctx context.Context) {
	j.controlCh <- 1
	j.job()
}

func TestSequentialPerIdWorkerQueue_ProcessAllJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	controlCh := make(chan uint64, 100)

	newJob := func(id uint64) *testJob {
		return &testJob{id, controlCh, func() {}}
	}

	queue := NewSequentialPerIdWorkerQueue(ctx, 2, 100, 100)

	for x := 0; x < 100; x++ {
		queue.Do(newJob(uint64((x % 2) + 1)))
	}

	count := uint64(0)
	for i := 0; i < 100; i++ {
		count += <-controlCh
	}

	assert.Equal(t, 100, int(count))
	assert.Len(t, queue.(*sequentialPerIdWorkerQueue).queue, 0)
	assert.Len(t, queue.(*sequentialPerIdWorkerQueue).sequentialQueue, 0)
	queue.Close()
}

func TestSequentialPerIdWorkerQueue_ProcessNotParallel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	controlCh := make(chan uint64, 100)
	c := make(map[uint64]int)
	c[1] = 0
	c[2] = 0

	newJob := func(id uint64) *testJob {
		return &testJob{id, controlCh, func() {
			c[id] += 1
		}}
	}

	queue := NewSequentialPerIdWorkerQueue(ctx, 2, 100, 100)

	for x := 0; x < 100; x++ {
		queue.Do(newJob(uint64((x % 2) + 1)))
	}

	count := uint64(0)
	for i := 0; i < 100; i++ {
		count += <-controlCh
	}

	assert.Equal(t, 100, int(count))
	assert.Equal(t, 50, c[1])
	assert.Equal(t, 50, c[2])
	assert.Len(t, queue.(*sequentialPerIdWorkerQueue).queue, 0)
	assert.Len(t, queue.(*sequentialPerIdWorkerQueue).sequentialQueue, 0)
	queue.Close()
}

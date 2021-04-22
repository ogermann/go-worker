package worker

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testJob struct {
	poolId	uint64
	controlCh chan uint64
}

func (j *testJob) Equals(obj interface{}) bool {
	cast, ok := obj.(*testJob)
	if !ok {
		return false
	}
	return j.poolId == cast.poolId
}

func (j *testJob) HashCode() int {
	return int(j.poolId)
}

func (j *testJob) Run(ctx context.Context) {
	time.Sleep(1 * time.Millisecond)
	j.controlCh <- j.poolId
}

func TestDistinctWorkerQueue_Basic(t *testing.T) {
	controlCh := make(chan uint64, 11)

	newJob := func(id uint64) *testJob {
		return &testJob{id, controlCh}
	}

	queue := NewDistinctWorkerQueue(1, 100)

	time.Sleep(1 * time.Millisecond)
	queue.Do(newJob(1))
	queue.Do(newJob(2))
	time.Sleep(1 * time.Millisecond)
	queue.Do(newJob(3))
	queue.Do(newJob(4))
	queue.Do(newJob(5))
	queue.Do(newJob(4))
	queue.Do(newJob(5))
	queue.Do(newJob(6))
	queue.Do(newJob(7))
	time.Sleep(1 * time.Millisecond)
	queue.Do(newJob(8))
	queue.Do(newJob(9))
	queue.Do(newJob(10))
	time.Sleep(1 * time.Millisecond)
	queue.Do(newJob(11))

	count := uint64(0)
	for i := 0; i < 11; i++ {
		count += <-controlCh
	}
	assert.Equal(t, 66, int(count))
	assert.True(t, queue.index.IsEmpty())
	queue.Close()
}
package raptor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maddiesch/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentWeightedRunner_Dequeue(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("WeightOrder", func(t *testing.T) {
		db := beginTx(t, conn)

		enqueueN(t, db, "critical", 3)
		enqueueN(t, db, "default", 3)

		r := NewConcurrentWeightedRunner(map[string]int{"critical": 2, "default": 1}, nil)
		r.Threads = 10 // larger than either weight, so batching can't hide an over-claim

		jobs := make(chan *Job, 10)
		err := r.dequeue(t.Context(), db, jobs)
		require.NoError(t, err)
		close(jobs)

		var got []string
		for j := range jobs {
			got = append(got, j.Queue)
		}

		assert.Equal(t, []string{"critical", "critical", "default"}, got)
	})

	t.Run("MovesOnWhenQueueEmptiesEarly", func(t *testing.T) {
		db := beginTx(t, conn)

		enqueueN(t, db, "critical", 2) // fewer jobs available than the configured weight
		enqueueN(t, db, "default", 1)

		r := NewConcurrentWeightedRunner(map[string]int{"critical": 100, "default": 10}, nil)
		r.Threads = 4

		jobs := make(chan *Job, 10)
		err := r.dequeue(t.Context(), db, jobs)
		require.NoError(t, err)
		close(jobs)

		var got []string
		for j := range jobs {
			got = append(got, j.Queue)
		}

		assert.Equal(t, []string{"critical", "critical", "default"}, got)
	})

	t.Run("WaitsWhenEmpty", func(t *testing.T) {
		db := beginTx(t, conn)

		r := NewConcurrentWeightedRunner(map[string]int{"default": 1}, nil)
		r.emptyCooldown = 50 * time.Millisecond

		jobs := make(chan *Job, 1)
		start := time.Now()
		err := r.dequeue(t.Context(), db, jobs)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	})

	t.Run("EmptyWaitInterruptedByContext", func(t *testing.T) {
		db := beginTx(t, conn)

		r := NewConcurrentWeightedRunner(map[string]int{"default": 1}, nil)
		r.emptyCooldown = 5 * time.Second

		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		jobs := make(chan *Job, 1)
		start := time.Now()
		err := r.dequeue(ctx, db, jobs)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Less(t, elapsed, 1*time.Second, "context cancellation should interrupt the cooldown wait early")
	})
}

// TestConcurrentWeightedRunner_Run shares one pool + migration across its
// subtests. Unlike the Dequeue subtests above, these run Run() with real
// worker goroutines issuing concurrent queries, so a single shared
// transaction (which only allows one in-flight query at a time) won't work.
// Each subtest uses its own queue names instead, so they stay isolated
// despite sharing the underlying database.
func TestConcurrentWeightedRunner_Run(t *testing.T) {
	db := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), db))

	t.Run("CompletesAllJobs", func(t *testing.T) {
		criticalIDs := enqueueN(t, db, "run-completes-critical", 5)
		defaultIDs := enqueueN(t, db, "run-completes-default", 5)
		want := append(append([]string{}, criticalIDs...), defaultIDs...)

		worker := &recordingWorker{}
		r := NewConcurrentWeightedRunner(
			map[string]int{"run-completes-critical": 2, "run-completes-default": 1},
			map[string]Worker{"test.job": worker},
		)
		r.Threads = 2
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return len(worker.ids()) == len(want)
		}, 3*time.Second, 10*time.Millisecond, "worker never received all jobs")

		cancel()
		require.NoError(t, <-done)

		assert.ElementsMatch(t, want, worker.ids())
	})

	t.Run("NoWorkerRegistered", func(t *testing.T) {
		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "run-noworker-default",
			Type:        "missing.type",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		r := NewConcurrentWeightedRunner(map[string]int{"run-noworker-default": 1}, map[string]Worker{})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return isDead(t, db, id)
		}, 2*time.Second, 10*time.Millisecond, "job was never marked dead")

		cancel()
		require.NoError(t, <-done)
	})

	t.Run("WorkerError", func(t *testing.T) {
		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "run-workererror-default",
			Type:        "test.job",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		worker := &recordingWorker{err: errors.New("boom")}
		r := NewConcurrentWeightedRunner(map[string]int{"run-workererror-default": 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return isDead(t, db, id)
		}, 2*time.Second, 10*time.Millisecond, "job was never marked dead")

		cancel()
		require.NoError(t, <-done)

		assert.Len(t, worker.ids(), 1)
	})
}

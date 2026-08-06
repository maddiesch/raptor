package raptor

import (
	"errors"
	"testing"

	"github.com/maddiesch/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerialRunner(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("Execute_DrainsQueuesInOrder", func(t *testing.T) {
		db := beginTx(t, conn)

		highIDs := enqueueN(t, db, "high", 2)
		lowIDs := enqueueN(t, db, "low", 2)

		worker := &recordingWorker{}
		r := NewSerialRunner([]string{"high", "low"}, map[string]Worker{"test.job": worker})

		err := r.Execute(t.Context(), db)
		require.NoError(t, err)

		got := worker.ids()
		require.Len(t, got, 4)
		assert.ElementsMatch(t, highIDs, got[:2], "high queue should be fully drained first")
		assert.ElementsMatch(t, lowIDs, got[2:], "low queue should run only after high is empty")

		for _, id := range append(append([]string{}, highIDs...), lowIDs...) {
			err := Complete(t.Context(), db, id)
			assert.ErrorIs(t, err, ErrJobNotFound, "job %s should already be completed by the runner", id)
		}
	})

	t.Run("Execute_NoWorkerRegistered", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "missing.type",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		r := NewSerialRunner([]string{"default"}, map[string]Worker{})

		err = r.Execute(t.Context(), db)
		require.NoError(t, err)

		assert.True(t, isDead(t, db, id))
	})

	t.Run("Execute_WorkerError", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		worker := &recordingWorker{err: errors.New("boom")}
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})

		err = r.Execute(t.Context(), db)
		require.NoError(t, err)

		assert.Len(t, worker.ids(), 1)
		assert.True(t, isDead(t, db, id))
	})

	t.Run("Execute_EmptyQueueList", func(t *testing.T) {
		db := beginTx(t, conn)

		r := NewSerialRunner(nil, nil)

		err := r.Execute(t.Context(), db)
		assert.NoError(t, err)
	})
}

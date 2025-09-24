package quill

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bedrock "github.com/yirzhou/bedrock"
)

func openTestStore(t *testing.T) *bedrock.KVStore {
	t.Helper()
	dir := t.TempDir()
	cfg := bedrock.NewDefaultConfiguration().
		WithBaseDir(filepath.Join(dir, "bedrock")).
		WithEnableMaintenance(false). // disable background loops in tests
		WithEnableCompaction(false).
		WithEnableCheckpoint(true).
		WithEnableSyncCheckpoint(false). // avoid fsync penalties in tests
		WithMemtableSizeThreshold(1024).
		WithCheckpointSize(1 << 20).
		WithNoLog()
	store, err := bedrock.Open(cfg)
	require.NoError(t, err, "failed to open bedrock store")
	t.Cleanup(func() {
		// Prefer clean up helper if available
		_ = store.CloseAndCleanUp()
		_ = os.RemoveAll(dir)
	})
	return store
}

func TestEnqueue_PersistsAndQueues(t *testing.T) {
	store := openTestStore(t)
	b := NewBroker(store)

	payload := []byte(`{"x":1}`)
	job, err := b.Enqueue("test_task", payload)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.NotEmpty(t, job.ID)

	// Verify it was written to the store with Pending status
	raw, ok := store.Get([]byte(job.ID))
	require.True(t, ok, "job not found in store by ID %s", job.ID)
	var stored Job
	require.NoError(t, json.Unmarshal(raw, &stored))
	assert.Equal(t, job.ID, stored.ID)
	assert.Equal(t, "test_task", stored.TaskName)
	assert.Equal(t, string(payload), string(stored.Payload))
	assert.Equal(t, Pending, stored.Status)
	assert.False(t, stored.CreatedAt.IsZero())

	// Verify the job ID was sent to the pending channel
	select {
	case got := <-b.pending:
		assert.Equal(t, job.ID, got)
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for job ID on pending channel")
	}
}

func TestEnqueue_MultipleJobs(t *testing.T) {
	store := openTestStore(t)
	b := NewBroker(store)

	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		job, err := b.Enqueue("task", []byte{byte('a' + i)})
		require.NoErrorf(t, err, "enqueue %d failed", i)
		ids = append(ids, job.ID)
	}

	// Assert each is present in store
	for i, id := range ids {
		raw, ok := store.Get([]byte(id))
		require.Truef(t, ok, "job %d not found in store: %s", i, id)
		var stored Job
		require.NoErrorf(t, json.Unmarshal(raw, &stored), "unmarshal job %d", i)
		assert.Equalf(t, Pending, stored.Status, "job %d status", i)
	}

	// Drain 3 IDs from channel (order should match send order since Enqueue sends immediately)
	got := make([]string, 0, 3)
	deadline := time.After(3 * time.Second)
	for len(got) < 3 {
		select {
		case id := <-b.pending:
			got = append(got, id)
		case <-deadline:
			t.Fatalf("timeout waiting for pending IDs, got %v", got)
		}
	}
	for i := range ids {
		assert.Equalf(t, ids[i], got[i], "channel order mismatch at %d", i)
	}
}

func TestDequeue_MarksRunningAndReturns(t *testing.T) {
	store := openTestStore(t)
	b := NewBroker(store)

	job, err := b.Enqueue("task", []byte("p"))
	require.NoError(t, err)

	got, err := b.Dequeue()
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, job.ID, got.ID)
	assert.Equal(t, Running, got.Status)

	// Verify DB state is Running
	raw, ok := store.Get([]byte(job.ID))
	require.True(t, ok)
	var stored Job
	require.NoError(t, json.Unmarshal(raw, &stored))
	assert.Equal(t, Running, stored.Status)
	assert.False(t, stored.UpdatedAt.IsZero())
}

func TestDequeue_SkipsMissingJobAndGetsNext(t *testing.T) {
	store := openTestStore(t)
	b := NewBroker(store)

	// Send a missing job ID first
	b.pending <- "missing-id-xyz"

	// Then enqueue a real job
	job, err := b.Enqueue("task", []byte("p2"))
	require.NoError(t, err)

	got, err := b.Dequeue()
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, job.ID, got.ID)
	assert.Equal(t, Running, got.Status)
}

func TestDequeue_SkipsNonPendingJob(t *testing.T) {
	store := openTestStore(t)
	b := NewBroker(store)

	// Create a job and mark it Completed (non-pending)
	nonPending, err := b.Enqueue("task", []byte("np"))
	require.NoError(t, err)
	raw, ok := store.Get([]byte(nonPending.ID))
	require.True(t, ok)
	var j Job
	require.NoError(t, json.Unmarshal(raw, &j))
	j.Status = Completed
	j.UpdatedAt = time.Now()
	buf, err := json.Marshal(&j)
	require.NoError(t, err)
	require.NoError(t, store.Put([]byte(j.ID), buf))

	// Queue the non-pending job ID first
	b.pending <- j.ID

	// Then enqueue a valid pending job
	valid, err := b.Enqueue("task", []byte("ok"))
	require.NoError(t, err)

	got, err := b.Dequeue()
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, valid.ID, got.ID)
	assert.Equal(t, Running, got.Status)

	// Ensure the non-pending job remains non-pending
	raw2, ok := store.Get([]byte(j.ID))
	require.True(t, ok)
	var j2 Job
	require.NoError(t, json.Unmarshal(raw2, &j2))
	assert.Equal(t, Completed, j2.Status)
}

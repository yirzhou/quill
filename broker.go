package quill

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	bedrock "github.com/yirzhou/bedrock"
)

// Broker is the central component that manages jobs.
type Broker struct {
	// A pointer to our Bedrock database for durable storage.
	db *bedrock.KVStore

	// A mutex to protect concurrent access to the broker's state.
	mu sync.Mutex

	// A channel that acts as the queue for pending job IDs.
	// Workers will listen on this channel to get new jobs.
	pending chan string
}

// DebugLogging gates verbose dequeue diagnostics.
var DebugLogging bool

// NewBroker constructs a Broker with a given Bedrock KV store and initializes
// its internal queue channel.
func NewBroker(db *bedrock.KVStore) *Broker {
	return &Broker{
		db:      db,
		pending: make(chan string, 1024),
	}
}

func generateUUID() string {
	return uuid.New().String()
}

// Enqueue accepts a new job, saves it durably, and queues it for execution.
func (b *Broker) Enqueue(taskName string, payload []byte) (*Job, error) {
	// 1. Create the full Job object.
	job := &Job{
		ID:        generateUUID(), // A helper to create a unique ID
		TaskName:  taskName,
		Payload:   payload,
		Status:    Pending,
		CreatedAt: time.Now(),
		// ... set other defaults like MaxRetries ...
	}

	// 2. Serialize the job to JSON or another format.
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}

	// 3. Save the job to Bedrock within a transaction.
	// This leverages the powerful feature you just built!
	txn := b.db.BeginTransaction()
	txn.Put([]byte(job.ID), jobBytes)
	if err := txn.Commit(); err != nil {
		// If the durable save fails, we can't proceed.
		return nil, err
	}

	// 4. If the save was successful, notify a worker by sending
	//    the job ID to the pending channel.
	b.pending <- job.ID

	log.Printf("Enqueued job %s with task '%s'", job.ID, job.TaskName)
	return job, nil
}

func (b *Broker) Dequeue() (*Job, error) {
	// Dequeue will loop until it can successfully find and claim a pending job.
	for {
		// 1. Wait for a notification that a job is available.
		jobID := <-b.pending
		if DebugLogging {
			fmt.Printf("Dequeue: received jobID=%s\n", jobID)
		}

		// 2. Acquire the Broker's lock to make the "find and claim" operation atomic.
		b.mu.Lock()

		// Use a closure with defer to ensure the lock is always released
		// before the loop continues or the function returns.
		job, err, shouldContinue := func() (*Job, error, bool) {
			if DebugLogging {
				fmt.Println("Dequeue: begin txn")
			}
			txn := b.db.BeginTransaction()

			// Use GetForUpdate to acquire a write lock and avoid RW lock upgrade deadlock.
			jobBytes, found := txn.GetForUpdate([]byte(jobID))
			if !found {
				// Job was likely deleted. This is not a fatal error for the worker.
				// Rollback, and signal to the outer loop to continue.
				txn.Rollback()
				log.Printf("Warning: Job %s not found in DB, skipping.", jobID)
				return nil, nil, true // continue = true
			}

			job := &Job{}
			if err := json.Unmarshal(jobBytes, job); err != nil {
				// This is a more serious error.
				txn.Rollback()
				return nil, err, false // continue = false
			}

			if DebugLogging {
				fmt.Printf("Dequeue: loaded job %s status=%v\n", job.ID, job.Status)
			}
			// 3. Verify the job's status.
			if job.Status != Pending {
				// Another worker claimed this job. This is normal.
				// Rollback and signal to the outer loop to continue.
				txn.Rollback()
				return nil, nil, true // continue = true
			}

			// 4. We found a pending job. Claim it.
			job.Status = Running
			job.UpdatedAt = time.Now()

			updatedJobBytes, err := json.Marshal(job)
			if err != nil {
				txn.Rollback()
				return nil, err, false
			}

			if DebugLogging {
				fmt.Println("Dequeue: put updated job")
			}
			if err := txn.Put([]byte(job.ID), updatedJobBytes); err != nil {
				txn.Rollback()
				return nil, err, false
			}

			// 5. Commit the transaction to durably mark the job as RUNNING.
			if DebugLogging {
				fmt.Println("Dequeue: commit txn")
			}
			if err := txn.Commit(); err != nil {
				return nil, err, false
			}
			if DebugLogging {
				fmt.Printf("Dequeue: claimed job %s as RUNNING\n", job.ID)
			}

			// Success! Return the job.
			return job, nil, false
		}()

		b.mu.Unlock() // CRITICAL: Unlock after the atomic operation is complete.

		if shouldContinue {
			continue // Go back to the top of the for loop to get the next jobID.
		}

		// If we are here, we either have a valid job or a fatal error.
		return job, err
	}
}

// ReportResult updates the job's status, result, and error message.
// The job's final state is durably persisted in the database.
func (b *Broker) ReportResult(jobID string, status JobStatus, result []byte, errorMsg string) error {

	txn := b.db.BeginTransaction()
	jobBytes, found := txn.Get([]byte(jobID))
	if !found {
		txn.Rollback()
		return fmt.Errorf("job not found")
	}

	job := &Job{}
	if err := json.Unmarshal(jobBytes, job); err != nil {
		txn.Rollback()
		return err
	}

	job.Status = status
	job.Result = result
	job.Error = errorMsg
	job.UpdatedAt = time.Now()

	updatedJobBytes, err := json.Marshal(job)
	if err != nil {
		txn.Rollback()
		return err
	}

	if err := txn.Put([]byte(job.ID), updatedJobBytes); err != nil {
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

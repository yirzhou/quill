package quill

import "time"

// JobStatus represents the state of a job in its lifecycle.
type JobStatus int

const (
	Pending   JobStatus = iota // The job is in the queue, waiting for a worker.
	Running                    // A worker has picked up the job and is executing it.
	Completed                  // The job finished successfully.
	Failed                     // The job failed after all retry attempts.
)

// Job is the fundamental unit of work in our system.
type Job struct {
	// 1. Unique Identifier (Your idea)
	ID string `json:"id"`

	// 2. Core Logic (Refined)
	TaskName string `json:"task_name"` // e.g., "SendWelcomeEmail"
	Payload  []byte `json:"payload"`   // The arguments for the task, e.g., a JSON blob of {"user_id": 123}

	// 3. Status and Result (Your idea, expanded)
	Status JobStatus `json:"status"`
	Result []byte    `json:"result"` // The output of the task if it was successful
	Error  string    `json:"error"`  // The error message if the task failed

	// 4. Scheduling and Timestamps (Expanding on your "Start time")
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	ScheduledFor time.Time `json:"scheduled_for"` // For jobs that should run in the future

	// 5. Reliability - New, essential fields
	MaxRetries     int `json:"max_retries"`
	CurrentAttempt int `json:"current_attempt"`
}

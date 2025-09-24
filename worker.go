package quill

import (
	"log"
	"time"

	"github.com/google/uuid" // A good library for unique IDs
)

// TaskFunc is the function signature for any task our worker can execute.
// It takes a payload and returns a result and/or an error.
type TaskFunc func(payload []byte) (result []byte, err error)

// Worker is a node that executes jobs.
type Worker struct {
	// A unique identifier for this specific worker, useful for logging.
	id string

	// A connection to the central Broker to get jobs and report results.
	broker *Broker

	// The "task registry": a map of registered task names to their
	// actual Go function implementations. This is the worker's "skill set."
	tasks map[string]TaskFunc
}

// NewWorker creates and initializes a new worker instance.
func NewWorker(broker *Broker) *Worker {
	return &Worker{
		id:     uuid.NewString(), // Assign a unique ID, e.g., "worker-af87..."
		broker: broker,
		tasks:  make(map[string]TaskFunc), // Initialize the task map
	}
}

// RegisterTask adds a new task to the worker's registry.
// This is how you "teach" a worker what functions it can run.
func (w *Worker) RegisterTask(name string, task TaskFunc) {
	log.Printf("Worker %s: Registered task '%s'", w.id, name)
	w.tasks[name] = task
}
func (w *Worker) Run() {
	log.Printf("Worker starting... Waiting for jobs.")
	for {
		// 1. Ask the Broker for a job. This is a blocking call.
		// It will wait until a job is available and has been durably
		// marked as "RUNNING" by the Broker.
		job, err := w.broker.Dequeue()
		if err != nil {
			log.Printf("Error dequeuing job: %v. Retrying in 5s.", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Picked up job %s (Task: %s)", job.ID, job.TaskName)

		// 2. Find the correct task function from our registry.
		taskFunc, ok := w.tasks[job.TaskName]
		if !ok {
			// This is a critical error. The Broker gave us a job we
			// don't know how to run. We must report it as a failure.
			log.Printf("Error: unknown task '%s' for job %s", job.TaskName, job.ID)
			// We need a way to report the result back to the Broker.
			w.broker.ReportResult(job.ID, Failed, nil, "unknown task name")
			continue
		}

		// 3. Execute the task.
		result, err := taskFunc(job.Payload)

		// 4. Report the outcome back to the Broker.
		if err != nil {
			log.Printf("Job %s failed: %v", job.ID, err)
			w.broker.ReportResult(job.ID, Failed, nil, err.Error())
		} else {
			log.Printf("Job %s completed successfully.", job.ID)
			w.broker.ReportResult(job.ID, Completed, result, "")
		}
	}
}

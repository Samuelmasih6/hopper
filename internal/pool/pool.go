// Package pool implements a fixed-size worker pool.
//
// This is the concurrency primitive the rest of GoQueue is built on top of.
// Everything later (Postgres-backed queue, retries, distributed workers)
// is really just a fancier version of "N goroutines pulling work off a
// channel" — so it's worth understanding this file cold.
package pool

import (
	"context"
	"sync"
	"time"
)

// Job is the unit of work a worker executes.
// For now it's deliberately simple (an ID + simulated duration) so we can
// reason about scheduling and shutdown behavior without any I/O in the way.
// Later this becomes a real job with a payload and a registered handler.
type Job struct {
	ID       int
	Duration time.Duration
}

// Result is what a worker reports after finishing a Job.
type Result struct {
	JobID    int
	WorkerID int
}

// Pool is a fixed set of worker goroutines reading from a shared jobs channel.
type Pool struct {
	numWorkers int
	jobs       chan Job
	results    chan Result
	wg         sync.WaitGroup
}

// New creates a Pool with numWorkers goroutines and a buffered jobs channel.
// bufferSize controls how many jobs can queue up before Submit blocks —
// this is a basic form of backpressure: if workers can't keep up, callers
// submitting jobs start blocking instead of memory growing unbounded.
func New(numWorkers, bufferSize int) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		jobs:       make(chan Job, bufferSize),
		results:    make(chan Result, bufferSize),
	}
}

// Start launches the worker goroutines. It returns immediately; workers run
// until ctx is cancelled or the jobs channel is closed and drained.
func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			// Context cancelled (timeout, or caller asked us to stop).
			// We return immediately WITHOUT finishing whatever's left in
			// the jobs channel. Real graceful shutdown needs more care
			// than this — we'll fix that in Phase 2. For today, the point
			// is to observe that this is what happens by default.
			return
		case job, ok := <-p.jobs:
			if !ok {
				// Channel closed and drained — no more work is coming.
				return
			}
			// Simulate work. A real handler call goes here later.
			time.Sleep(job.Duration)
			p.results <- Result{JobID: job.ID, WorkerID: id}
		}
	}
}

// Submit adds a job to the queue. Blocks if the buffer is full (backpressure).
func (p *Pool) Submit(j Job) {
	p.jobs <- j
}

// CloseJobs signals that no more jobs will be submitted. Workers finish
// whatever's already in the channel, then exit.
func (p *Pool) CloseJobs() {
	close(p.jobs)
}

// Wait blocks until all worker goroutines have exited.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// Results returns the channel workers publish results to.
func (p *Pool) Results() <-chan Result {
	return p.results
}

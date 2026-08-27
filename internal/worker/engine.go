// Package worker connects Day 1's concurrency model to Day 2's persistence
// layer. There's a real design shift happening here worth naming explicitly:
//
// Day 1's Pool had an in-memory channel as the queue, and a separate
// goroutine fed jobs into it. Workers just received from the channel.
//
// Here, there IS no in-memory queue. Postgres *is* the queue. Each worker
// independently calls store.Claim() whenever it wants work — there's no
// central dispatcher handing jobs out. This is a "pull" model instead of
// "push," and it's what makes horizontal scaling trivial later: adding a
// worker on another machine in Phase 3 just means pointing another process
// at the same database and calling Claim() — no new coordination code.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Samuelmasih6/hopper/internal/store"
)

// HandlerFunc processes one job's payload. Returning an error marks the job
// failed (and eligible for retry, per Day 2's MarkFailed logic); returning
// nil marks it succeeded.
type HandlerFunc func(ctx context.Context, payload json.RawMessage) error

// Registry maps a queue name to the handler responsible for jobs on it.
//
// Design simplification, worth being upfront about: we're using "queue"
// to mean both (a) which jobs a worker pool listens to, AND (b) which
// handler function processes them. Real systems (Sidekiq, Celery) usually
// separate "queue" (a priority/routing concept) from "job class/type"
// (which code runs). We're collapsing them for now to keep Day 3 focused —
// this is a seam we may revisit if a later phase needs finer routing.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]HandlerFunc)}
}

func (r *Registry) Register(queue string, h HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[queue] = h
}

func (r *Registry) get(queue string) (HandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[queue]
	return h, ok
}

// Engine runs a fixed pool of workers that pull jobs from the store and
// dispatch them via the Registry.
type Engine struct {
	store        *store.Store
	registry     *Registry
	numWorkers   int
	pollInterval time.Duration
	wg           sync.WaitGroup
}

func New(s *store.Store, r *Registry, numWorkers int, pollInterval time.Duration) *Engine {
	return &Engine{
		store:        s,
		registry:     r,
		numWorkers:   numWorkers,
		pollInterval: pollInterval,
	}
}

// Start launches numWorkers goroutines and returns immediately.
func (e *Engine) Start(ctx context.Context) {
	for i := 1; i <= e.numWorkers; i++ {
		e.wg.Add(1)
		workerID := fmt.Sprintf("worker-%d", i)
		go e.loop(ctx, workerID)
	}
}

// Wait blocks until every worker goroutine has exited — i.e., until they've
// all noticed ctx is cancelled and finished whatever job they were mid-run
// on. This is Day 3's fix for Day 1's Scenario 2 problem: a worker never
// abandons a job it's already claimed. It just stops claiming NEW ones.
//
// This is NOT a complete answer to "what if a worker dies while holding a
// job" — that's a worker process disappearing entirely (crash, OOM-kill),
// which no amount of graceful-shutdown code inside that same process can
// handle. That's still Phase 2's visibility-timeout problem. What we've
// fixed here is specifically the *voluntary, graceful* shutdown case.
func (e *Engine) Wait() {
	e.wg.Wait()
}

func (e *Engine) loop(ctx context.Context, workerID string) {
	defer e.wg.Done()
	for {
		// Check for cancellation BEFORE claiming a new job — if the
		// context is already done, don't start new work.
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := e.store.Claim(ctx, workerID)
		if err != nil {
			if errors.Is(err, store.ErrNoJob) {
				// Nothing to do. Back off before polling again rather
				// than hammering Postgres in a tight loop — but note we
				// do NOT back off after successfully processing a job
				// (see below), so a backlog drains at full speed and we
				// only go idle-friendly once the queue is actually empty.
				if !sleep(ctx, e.pollInterval) {
					return
				}
				continue
			}
			// A real error talking to Postgres (connection dropped, etc).
			// Back off the same way — hammering a struggling database
			// would make things worse, not better.
			log.Printf("[%s] claim error: %v", workerID, err)
			if !sleep(ctx, e.pollInterval) {
				return
			}
			continue
		}

		e.run(ctx, workerID, job)
		// No sleep here — immediately try to claim the next job. If
		// there's a backlog, workers should drain it as fast as they can.
	}
}

func (e *Engine) run(ctx context.Context, workerID string, job *store.Job) {
	handler, ok := e.registry.get(job.Queue)
	if !ok {
		msg := fmt.Sprintf("no handler registered for queue %q", job.Queue)
		log.Printf("[%s] job %d: %s", workerID, job.ID, msg)
		if err := e.store.MarkFailed(ctx, job.ID, msg); err != nil {
			log.Printf("[%s] job %d: mark failed error: %v", workerID, job.ID, err)
		}
		return
	}

	err := handler(ctx, job.Payload)
	if err != nil {
		log.Printf("[%s] job %d failed: %v", workerID, job.ID, err)
		if merr := e.store.MarkFailed(ctx, job.ID, err.Error()); merr != nil {
			log.Printf("[%s] job %d: mark failed error: %v", workerID, job.ID, merr)
		}
		return
	}

	if err := e.store.MarkSucceeded(ctx, job.ID); err != nil {
		log.Printf("[%s] job %d: mark succeeded error: %v", workerID, job.ID, err)
	}
}

// sleep waits for d, or returns false early if ctx is cancelled first —
// lets a worker respond to shutdown immediately instead of finishing out
// a full poll interval it doesn't need to.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

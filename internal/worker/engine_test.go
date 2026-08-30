package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Samuelmasih6/hopper/internal/store"
)

func testStoreForEngine(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("HOPPER_TEST_DSN")
	if dsn == "" {
		t.Skip("HOPPER_TEST_DSN not set, skipping engine integration tests")
	}
	s, err := store.Open(dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestEngineProcessesJobsEndToEnd is the whole point of Day 3: prove a job
// enqueued through the store actually gets claimed, run through a real
// handler, and marked succeeded — with multiple workers pulling concurrently.
func TestEngineProcessesJobsEndToEnd(t *testing.T) {
	s := testStoreForEngine(t)
	ctx := context.Background()

	var processed int64
	reg := NewRegistry()
	reg.Register("default", func(ctx context.Context, payload json.RawMessage) error {
		atomic.AddInt64(&processed, 1)
		return nil
	})

	const n = 15
	var ids []int64
	for i := 0; i < n; i++ {
		payload, _ := json.Marshal(map[string]int{"i": i})
		job, err := s.Enqueue(ctx, "default", "process_item", payload)
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		ids = append(ids, job.ID)
	}

	engine := New(s, reg, 4, 20*time.Millisecond)
	runCtx, cancel := context.WithCancel(ctx)
	engine.Start(runCtx)

	// Poll until all jobs are processed, or time out — avoids a fixed
	// sleep that's either too short (flaky) or too long (slow test).
	deadline := time.After(5 * time.Second)
	for atomic.LoadInt64(&processed) < n {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for jobs to process; got %d/%d", processed, n)
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	engine.Wait()

	for _, id := range ids {
		job, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("get job %d: %v", id, err)
		}
		if job.Status != "succeeded" {
			t.Errorf("job %d: expected succeeded, got %s", id, job.Status)
		}
	}
}

// TestEngineMarksFailedJobs proves a handler error correctly flows through
// to MarkFailed, and that a job exhausts its retries as expected.
func TestEngineMarksFailedJobs(t *testing.T) {
	s := testStoreForEngine(t)
	ctx := context.Background()

	reg := NewRegistry()
	reg.Register("flaky", func(ctx context.Context, payload json.RawMessage) error {
		return errors.New("simulated failure")
	})

	payload, _ := json.Marshal(map[string]string{"task": "will_fail"})
	job, err := s.Enqueue(ctx, "flaky", "flaky_task", payload)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Deliberately NOT overriding max_attempts (default 5) — this proves
	// the natural retry-then-exhaust path: the engine has no backoff yet
	// (that's Phase 2), so a failed job goes straight back to "pending"
	// and gets reclaimed almost immediately. We're relying on exactly
	// that behavior to reach "failed" within a handful of attempts.

	engine := New(s, reg, 1, 20*time.Millisecond)
	runCtx, cancel := context.WithCancel(ctx)
	engine.Start(runCtx)

	deadline := time.After(3 * time.Second)
	for {
		got, err := s.Get(ctx, job.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status == "failed" {
			if !got.LastError.Valid || got.LastError.String != "simulated failure" {
				t.Errorf("expected last_error to be recorded, got %+v", got.LastError)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for job to reach failed status, last status: %s", got.Status)
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	engine.Wait()
}

// TestEngineUnknownQueueMarksFailed proves a job on a queue with no
// registered handler doesn't hang forever — it fails loudly and immediately.
func TestEngineUnknownQueueMarksFailed(t *testing.T) {
	s := testStoreForEngine(t)
	ctx := context.Background()

	reg := NewRegistry() // nothing registered

	payload, _ := json.Marshal(map[string]string{"task": "orphan"})
	job, err := s.Enqueue(ctx, "nobody_listens_here", "orphan_task", payload)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Same as above: no handler ever succeeds here, so this also relies
	// on retry exhaustion (default max_attempts=5) to reach "failed."

	engine := New(s, reg, 1, 20*time.Millisecond)
	runCtx, cancel := context.WithCancel(ctx)
	engine.Start(runCtx)
	defer func() {
		cancel()
		engine.Wait()
	}()

	deadline := time.After(3 * time.Second)
	for {
		got, err := s.Get(ctx, job.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status == "failed" {
			return // success
		}
		select {
		case <-deadline:
			t.Fatalf("timed out; last status: %s", got.Status)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

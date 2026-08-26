package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
)

// testDSN reads from env so this works both locally and in CI without
// hardcoding credentials. Skips the whole file's tests if not set, rather
// than failing — you don't want `go test ./...` to break for someone who
// hasn't got Postgres running.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("HOPPER_TEST_DSN")
	if dsn == "" {
		t.Skip("HOPPER_TEST_DSN not set, skipping store integration tests")
	}
	s, err := Open(dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestEnqueueAndClaim(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	payload, _ := json.Marshal(map[string]string{"task": "send_email"})
	job, err := s.Enqueue(ctx, "default", payload)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if job.Status != "pending" {
		t.Fatalf("expected pending, got %s", job.Status)
	}

	claimed, err := s.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != job.ID {
		t.Fatalf("claimed wrong job: got %d want %d", claimed.ID, job.ID)
	}
	if claimed.Status != "running" {
		t.Fatalf("expected running, got %s", claimed.Status)
	}
	if claimed.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", claimed.Attempts)
	}
}

func TestClaimReturnsErrNoJobWhenEmpty(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Drain anything left over from other tests in this run.
	for {
		_, err := s.Claim(ctx, "drainer")
		if err == ErrNoJob {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error while draining: %v", err)
		}
	}

	_, err := s.Claim(ctx, "worker-1")
	if err != ErrNoJob {
		t.Fatalf("expected ErrNoJob, got %v", err)
	}
}

// TestConcurrentClaimsNeverDuplicate is the important one. It enqueues N
// jobs, then fires N goroutines at Claim() simultaneously — if SKIP LOCKED
// weren't doing its job, we'd expect to see the same job ID handed to more
// than one goroutine, or workers blocking/erroring into each other. Neither
// should happen: every job should be claimed by exactly one goroutine.
func TestConcurrentClaimsNeverDuplicate(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	const n = 20
	seeded := make(map[int64]bool)
	for i := 0; i < n; i++ {
		payload, _ := json.Marshal(map[string]int{"i": i})
		job, err := s.Enqueue(ctx, "default", payload)
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		seeded[job.ID] = true
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = make(map[int64]int) // job ID -> number of times claimed
	)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			job, err := s.Claim(ctx, fmt.Sprintf("worker-%d", workerNum))
			if err != nil {
				t.Errorf("worker %d: claim: %v", workerNum, err)
				return
			}
			mu.Lock()
			claimed[job.ID]++
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if len(claimed) != n {
		t.Fatalf("expected %d distinct jobs claimed, got %d", n, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Errorf("job %d was claimed %d times — SKIP LOCKED failed to prevent duplicate claim", id, count)
		}
		if !seeded[id] {
			t.Errorf("claimed job %d that we never seeded", id)
		}
	}
}

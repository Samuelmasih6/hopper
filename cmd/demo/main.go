package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Samuelmasih6/hopper/internal/pool"
)

func main() {
	fmt.Println("=== Scenario 1: normal run, 3 workers, 10 jobs ===")
	normalRun()

	fmt.Println()
	fmt.Println("=== Scenario 2: context cancelled mid-run ===")
	cancellationRun()
}

// normalRun shows jobs completing out of submission order — proof that
// the pool is genuinely running work concurrently, not one-at-a-time.
func normalRun() {
	ctx := context.Background()
	p := pool.New(3, 10)
	p.Start(ctx)

	go func() {
		for i := 1; i <= 10; i++ {
			// Random duration so completion order isn't predictable.
			d := time.Duration(rand.Intn(200)) * time.Millisecond
			p.Submit(pool.Job{ID: i, Duration: d})
		}
		p.CloseJobs()
	}()

	// Drain results as they arrive. We stop reading once all workers exit
	// AND the results channel is empty — simplest way to do that here is
	// to close results after Wait() in a separate goroutine, then range.
	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()

	count := 0
	for count < 10 {
		r := <-p.Results()
		fmt.Printf("  job %d finished by worker %d\n", r.JobID, r.WorkerID)
		count++
	}
	<-done
}

// cancellationRun demonstrates the default (naive) shutdown behavior:
// when the context is cancelled, workers stop immediately, even if jobs
// are still sitting in the channel. This is intentionally "wrong" for a
// production queue — we're establishing the baseline we'll improve on
// in Phase 2 when we add proper graceful shutdown.
func cancellationRun() {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	p := pool.New(2, 20)
	p.Start(ctx)

	go func() {
		for i := 1; i <= 20; i++ {
			p.Submit(pool.Job{ID: i, Duration: 50 * time.Millisecond})
		}
		p.CloseJobs()
	}()

	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()

	completed := 0
loop:
	for {
		select {
		case r, ok := <-p.Results():
			if !ok {
				break loop
			}
			fmt.Printf("  job %d finished by worker %d\n", r.JobID, r.WorkerID)
			completed++
		case <-done:
			break loop
		}
	}
	fmt.Printf("  --> only %d/20 jobs completed before context timeout\n", completed)
}

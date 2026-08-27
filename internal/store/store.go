// Package store is the persistence layer for Hopper. This is what makes
// jobs survive a crash — Day 1's in-memory channel couldn't.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq" // registers the "postgres" driver with database/sql
)

// ErrNoJob is returned by Claim when there's nothing pending to work on.
// It's a sentinel error (not a wrapped one) because "no work available" is
// an expected, routine outcome — callers check for it with errors.Is, not
// by inspecting error text.
var ErrNoJob = errors.New("store: no job available")

// Job mirrors the jobs table. Payload is kept as raw JSON bytes rather than
// unmarshaled into a concrete type here — the store doesn't know or care
// what a job's payload means; that's the handler's job (literally), later.
type Job struct {
	ID          int64
	Queue       string
	Payload     json.RawMessage
	Status      string
	Attempts    int
	MaxAttempts int
	LockedBy    sql.NullString
	LockedAt    sql.NullTime
	LastError   sql.NullString
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Store wraps a *sql.DB. We depend on database/sql's connection pool rather
// than managing connections ourselves — it already handles the hard parts
// (idle connections, max open connections, retrying dead connections).
type Store struct {
	db *sql.DB
}

// Open connects to Postgres and verifies the connection with a Ping.
// Failing fast here (instead of on the first query later) means startup
// errors show up at startup, not confusingly deep into a request.
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// Enqueue inserts a new pending job and returns it with its assigned ID.
func (s *Store) Enqueue(ctx context.Context, queue string, payload json.RawMessage) (*Job, error) {
	const q = `
		INSERT INTO jobs (queue, payload)
		VALUES ($1, $2)
		RETURNING id, queue, payload, status, attempts, max_attempts,
		          locked_by, locked_at, last_error, created_at, updated_at
	`
	row := s.db.QueryRowContext(ctx, q, queue, payload)
	return scanJob(row)
}

// Claim atomically finds one pending job, marks it "running" and locked by
// workerID, and returns it — all in one round trip. This is the single most
// important query in the whole system, so read the comment below carefully.
//
// The core trick is:
//
//	SELECT ... FOR UPDATE SKIP LOCKED
//
// FOR UPDATE takes a row-level lock on whatever the SELECT finds, for the
// duration of the transaction — normally, a SECOND transaction running the
// same SELECT concurrently would just BLOCK, waiting for the first lock to
// release, and then lock the SAME row once it can. That's wrong for us: two
// workers would serialize on claiming, and the second would try to "claim"
// a job the first one already took.
//
// SKIP LOCKED changes that: instead of blocking on a locked row, the query
// skips it and finds the next one that ISN'T locked. So N workers running
// this concurrently naturally fan out across N different jobs, with no
// external coordination, no distributed lock service, nothing — Postgres's
// own MVCC machinery does it for free. This is exactly how SQS-style visible
// queues behave, and it's the standard technique for Postgres-as-queue.
func (s *Store) Claim(ctx context.Context, workerID string) (*Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer tx.Rollback() // no-op if we commit successfully below

	const selectQ = `
		SELECT id FROM jobs
		WHERE status = 'pending'
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`
	var id int64
	if err := tx.QueryRowContext(ctx, selectQ).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoJob
		}
		return nil, fmt.Errorf("store: select for claim: %w", err)
	}

	const updateQ = `
		UPDATE jobs
		SET status = 'running',
		    attempts = attempts + 1,
		    locked_by = $1,
		    locked_at = now(),
		    updated_at = now()
		WHERE id = $2
		RETURNING id, queue, payload, status, attempts, max_attempts,
		          locked_by, locked_at, last_error, created_at, updated_at
	`
	row := tx.QueryRowContext(ctx, updateQ, workerID, id)
	job, err := scanJob(row)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit claim: %w", err)
	}
	return job, nil
}

// MarkSucceeded transitions a job to its terminal success state.
func (s *Store) MarkSucceeded(ctx context.Context, id int64) error {
	const q = `UPDATE jobs SET status = 'succeeded', updated_at = now() WHERE id = $1`
	_, err := s.db.ExecContext(ctx, q, id)
	if err != nil {
		return fmt.Errorf("store: mark succeeded: %w", err)
	}
	return nil
}

// MarkFailed transitions a job back to pending (if attempts remain) or to
// the terminal "failed" state (if it's out of attempts). This is a stub for
// Day 2 — no backoff delay yet, that's Phase 2 — but the attempts vs
// max_attempts branch is the real shape retries will build on.
func (s *Store) MarkFailed(ctx context.Context, id int64, cause string) error {
	const q = `
		UPDATE jobs
		SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
		    last_error = $2,
		    updated_at = now()
		WHERE id = $1
	`
	_, err := s.db.ExecContext(ctx, q, id, cause)
	if err != nil {
		return fmt.Errorf("store: mark failed: %w", err)
	}
	return nil
}

// ClaimSingleQuery is functionally identical to Claim, but does the whole
// select-and-update in one statement via a CTE, instead of an explicit
// BEGIN/SELECT/UPDATE/COMMIT. One network round trip instead of several.
//
// Why this is still atomic without an explicit transaction: a single SQL
// statement in Postgres is ALWAYS atomic on its own — every statement runs
// inside an implicit transaction if you don't start one explicitly. The CTE
// (`next_job`) and the UPDATE that consumes it execute as one indivisible
// unit, so there's no window between "find the row" and "lock it" for
// another connection to interleave, same guarantee as the multi-statement
// version.
//
// Trade-off versus Claim: this version is harder to read for someone
// learning the mechanism for the first time (the locking and the update are
// visually tangled together), which is why the explicit-transaction version
// stays as the primary implementation. This one is here to prove the
// equivalence and as the "how would you make this faster" answer.
func (s *Store) ClaimSingleQuery(ctx context.Context, workerID string) (*Job, error) {
	const q = `
		WITH next_job AS (
			SELECT id FROM jobs
			WHERE status = 'pending'
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs
		SET status = 'running',
		    attempts = attempts + 1,
		    locked_by = $1,
		    locked_at = now(),
		    updated_at = now()
		FROM next_job
		WHERE jobs.id = next_job.id
		RETURNING jobs.id, jobs.queue, jobs.payload, jobs.status, jobs.attempts,
		          jobs.max_attempts, jobs.locked_by, jobs.locked_at, jobs.last_error,
		          jobs.created_at, jobs.updated_at
	`
	row := s.db.QueryRowContext(ctx, q, workerID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoJob
		}
		return nil, err
	}
	return job, nil
}

// row is satisfied by both *sql.Row and *sql.Row from a transaction —
// lets scanJob be shared between Enqueue (plain db) and Claim (tx).
type row interface {
	Scan(dest ...any) error
}

func scanJob(r row) (*Job, error) {
	var j Job
	err := r.Scan(
		&j.ID, &j.Queue, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.LockedBy, &j.LockedAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("store: scan job: %w", err)
	}
	return &j, nil
}

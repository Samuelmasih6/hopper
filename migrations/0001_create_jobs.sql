-- 0001_create_jobs.sql
--
-- Design notes (read this before touching the schema):
--
-- status: a plain TEXT enum-ish column rather than Postgres ENUM type.
--   ENUMs are annoying to alter later (adding a value requires care around
--   transactions), and we WILL add statuses (e.g. "dead") in Phase 2.
--   A CHECK constraint gives us safety without the migration pain.
--
-- attempts / max_attempts: attempts is incremented every time a worker
--   claims the job, not every time it fails. This matters — a worker that
--   crashes without reporting failure still counts as an attempt, which is
--   the correct behavior (Phase 2 will lean on this for the visibility
--   timeout mechanism).
--
-- locked_by / locked_at: nullable. NULL means "unclaimed." When a worker
--   claims a job we stamp both. Today (Day 2) nothing ever clears a stale
--   lock — that's tomorrow's problem (visibility timeout), left visible
--   here on purpose so you see exactly where Phase 2 will hook in.
--
-- The partial index only covers status = 'pending' rows: that's the ONLY
-- set of rows workers scan when looking for work, and it's kept small
-- because completed/failed jobs age out of it entirely. This is a classic
-- "index only what you actually query" decision.

CREATE TABLE jobs (
    id              BIGSERIAL PRIMARY KEY,
    queue           TEXT NOT NULL DEFAULT 'default',
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 5,
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Workers only ever look for pending jobs — index just that slice.
CREATE INDEX idx_jobs_pending ON jobs (created_at) WHERE status = 'pending';

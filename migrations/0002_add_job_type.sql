-- 0002_add_job_type.sql
--
-- "queue" answers "which pool of workers handles this" (useful later for
-- e.g. routing high-priority jobs to a dedicated pool). "type" answers
-- "which Go function actually runs it" — a worker needs to look up a
-- handler by type before it can do anything with the payload.
--
-- Conflating these two would make it impossible to, say, run "send_email"
-- jobs on both a "default" queue and a "high_priority" queue with the same
-- handler — keeping them separate columns keeps that door open.
--
-- NOT NULL with no default going forward: every future INSERT must specify
-- a type explicitly — there's no sane default job type, an unspecified
-- type is a bug in the caller, not something to paper over.
--
-- BUT: this table already has rows (17, in dev — imagine millions in
-- production). `ADD COLUMN type TEXT NOT NULL` with no default fails
-- immediately, because Postgres has no value to put in existing rows and
-- refuses to violate the constraint it's being asked to add. This is a
-- real, common migration mistake — the fix is the standard three-step
-- pattern:
--   1. Add the column as NULLABLE (instant, no table rewrite in PG 11+)
--   2. Backfill existing rows with a real value
--   3. THEN add the NOT NULL constraint, now that it's actually satisfied
-- Doing it in three statements instead of one is what makes this safe to
-- run against a live table with real traffic, not just a toy dev database.

ALTER TABLE jobs ADD COLUMN type TEXT;

UPDATE jobs SET type = 'unknown' WHERE type IS NULL;

ALTER TABLE jobs ALTER COLUMN type SET NOT NULL;

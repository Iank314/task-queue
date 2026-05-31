CREATE TABLE jobs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type         TEXT NOT NULL,
    payload      JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    priority     INT NOT NULL DEFAULT 0,
    attempts     INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    result       JSONB,
    error        TEXT,
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now(),
    run_after        TIMESTAMPTZ DEFAULT now(),
    lease_expires_at TIMESTAMPTZ
);

CREATE INDEX idx_jobs_claimable
    ON jobs (priority DESC, created_at ASC)
    WHERE status = 'pending';

-- Lets the reaper cheaply find running jobs whose lease has lapsed.
CREATE INDEX idx_jobs_expired_lease
    ON jobs (lease_expires_at)
    WHERE status = 'running';

CREATE TABLE dead_letter_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_job_id UUID NOT NULL,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    priority        INT NOT NULL DEFAULT 0,
    attempts        INT NOT NULL,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    failed_at       TIMESTAMPTZ DEFAULT now()
);

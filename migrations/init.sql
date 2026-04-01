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
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now(),
    run_after    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_jobs_claimable
    ON jobs (priority DESC, created_at ASC)
    WHERE status = 'pending';

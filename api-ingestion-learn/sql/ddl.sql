CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Per-source incremental state. Only advanced after a run fully succeeds (incl. DQ gate).
CREATE TABLE IF NOT EXISTS ops.pipeline_state (
    source      VARCHAR PRIMARY KEY,
    watermark   VARCHAR,      -- max updated_at seen, ISO-8601 Z (provider's own format, lossless)
    etag_key    VARCHAR,      -- which request the etag belongs to (path + params)
    etag        VARCHAR,
    updated_at  TIMESTAMP DEFAULT current_timestamp
);

-- One row per run: the first place anyone looks when "the dashboard is stale".
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id          VARCHAR PRIMARY KEY,
    source          VARCHAR,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          VARCHAR,  -- running | success | no_change | failed
    rows_fetched    INTEGER,
    rows_inserted   INTEGER,
    rows_updated    INTEGER,
    rows_rejected   INTEGER,
    http_requests   INTEGER,
    http_retries    INTEGER,
    error           VARCHAR
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    run_id    VARCHAR,
    check_name VARCHAR,
    severity  VARCHAR,
    passed    BOOLEAN,
    detail    VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.issues (
    id               BIGINT PRIMARY KEY,
    repo             VARCHAR,
    number           INTEGER,
    title            VARCHAR,
    state            VARCHAR,
    author_id        BIGINT,
    author_login     VARCHAR,
    labels           VARCHAR[],
    comments         INTEGER,
    is_pull_request  BOOLEAN,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    closed_at        TIMESTAMP,
    _run_id          VARCHAR,
    _loaded_at       TIMESTAMP
);

-- Records that failed the contract. Never silently dropped.
CREATE TABLE IF NOT EXISTS silver.dead_letter (
    run_id      VARCHAR,
    source      VARCHAR,
    payload     VARCHAR,
    error       VARCHAR,
    rejected_at TIMESTAMP
);

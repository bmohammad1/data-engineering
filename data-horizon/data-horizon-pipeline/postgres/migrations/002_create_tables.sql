-- Migration 002: Create pipeline_runs and pipeline_tags tables
-- Replaces the DynamoDB PipelineAudit single-table design.
-- Run after 001_create_schema.sql.

-- ============================================================
-- Table: pipeline_audit.pipeline_runs
-- Replaces: DynamoDB items with SK = "META"
-- Replaces: GSI1_RunByPipeline index (add indexes separately via 003)
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_audit.pipeline_runs (
    run_id                       TEXT        NOT NULL,

    pipeline_name                TEXT        NOT NULL DEFAULT 'data_horizon',
    environment                  TEXT        NOT NULL
                                     CHECK (environment IN ('dev', 'staging', 'prod')),
    trigger_type                 TEXT        NOT NULL DEFAULT 'schedule',
    total_tags                   INTEGER     NOT NULL DEFAULT 0,

    overall_status               TEXT        NOT NULL DEFAULT 'PENDING'
                                     CHECK (overall_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),

    start_time                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time                     TIMESTAMPTZ,

    -- Config stage (populated by write_config_stage_end)
    config_end_time              TIMESTAMPTZ,
    config_duration_ms           BIGINT,

    -- Transform stage (populated by update_run_transform_status)
    transform_status             TEXT
                                     CHECK (transform_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    transform_tags_success       INTEGER     DEFAULT 0,
    transform_tags_failed        INTEGER     DEFAULT 0,
    transform_records_extracted  BIGINT      DEFAULT 0,
    transform_records_written    BIGINT      DEFAULT 0,
    transform_records_dropped    BIGINT      DEFAULT 0,
    transform_duration_ms        BIGINT      DEFAULT 0,

    -- Validate stage (populated by update_run_validate_status)
    validate_status              TEXT
                                     CHECK (validate_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    validate_tags_success        INTEGER     DEFAULT 0,
    validate_tags_failed         INTEGER     DEFAULT 0,
    validate_records_passed      BIGINT      DEFAULT 0,
    validate_records_quarantined BIGINT      DEFAULT 0,
    validate_duration_ms         BIGINT      DEFAULT 0,

    -- TTL equivalent: column kept for future cleanup job; no daemon configured yet
    expires_at                   TIMESTAMPTZ NOT NULL
                                     GENERATED ALWAYS AS (created_at + INTERVAL '30 days') STORED,

    PRIMARY KEY (run_id)
);

-- ============================================================
-- Table: pipeline_audit.pipeline_tags
-- Replaces: DynamoDB items with SK = "TAG#<tag_id>"
-- The nested stage_status map { EXTRACT, TRANSFORM, VALIDATE }
-- becomes three flat TEXT columns with CHECK constraints.
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_audit.pipeline_tags (
    run_id                       TEXT        NOT NULL
                                     REFERENCES pipeline_audit.pipeline_runs(run_id)
                                     ON DELETE CASCADE,
    tag_key                      TEXT        NOT NULL,

    pipeline_name                TEXT        NOT NULL DEFAULT 'data_horizon',
    endpoint                     TEXT        NOT NULL,

    overall_status               TEXT        NOT NULL DEFAULT 'PENDING'
                                     CHECK (overall_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),

    -- Replaces DynamoDB stage_status map { EXTRACT, TRANSFORM, VALIDATE }
    stage_extract_status         TEXT        NOT NULL DEFAULT 'PENDING'
                                     CHECK (stage_extract_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    stage_transform_status       TEXT        NOT NULL DEFAULT 'PENDING'
                                     CHECK (stage_transform_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    stage_validate_status        TEXT        NOT NULL DEFAULT 'PENDING'
                                     CHECK (stage_validate_status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),

    -- Extract stage metrics
    records_received             BIGINT      NOT NULL DEFAULT 0,
    extraction_duration_ms       BIGINT      NOT NULL DEFAULT 0,
    attempts                     INTEGER     NOT NULL DEFAULT 0,

    -- Transform stage metrics
    transform_records_extracted  BIGINT      NOT NULL DEFAULT 0,
    transform_records_dropped    BIGINT      NOT NULL DEFAULT 0,
    transform_records_written    BIGINT      NOT NULL DEFAULT 0,
    transform_duration_ms        BIGINT      NOT NULL DEFAULT 0,

    -- Validate stage metrics
    validate_records_passed      BIGINT      NOT NULL DEFAULT 0,
    validate_records_quarantined BIGINT      NOT NULL DEFAULT 0,
    validate_duration_ms         BIGINT      NOT NULL DEFAULT 0,

    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at                   TIMESTAMPTZ NOT NULL
                                     GENERATED ALWAYS AS (created_at + INTERVAL '30 days') STORED,

    PRIMARY KEY (run_id, tag_key)
);

-- =============================================================================
-- UNIVERSITY DATA WAREHOUSE — SCHEMA CREATION
-- Supports: Snowflake (prod) and PostgreSQL (local staging)
-- =============================================================================

-- ── Databases / Schemas ───────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS UNIVERSITY_DW;
USE DATABASE UNIVERSITY_DW;

CREATE SCHEMA IF NOT EXISTS STAGING  COMMENT = 'Raw ingested data before transformation';
CREATE SCHEMA IF NOT EXISTS PROD     COMMENT = 'Star-schema production data warehouse';
CREATE SCHEMA IF NOT EXISTS AUDIT    COMMENT = 'Data quality, lineage, and change logs';

-- ── Virtual Warehouses (Snowflake) ────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS UNIVERSITY_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Primary ETL + query warehouse';

CREATE WAREHOUSE IF NOT EXISTS REPORTING_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Dashboard and ad-hoc reporting';

-- ── File Formats ──────────────────────────────────────────────────────────────
USE SCHEMA STAGING;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE             = CSV
    FIELD_DELIMITER  = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER      = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF          = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE         = JSON
    STRIP_OUTER_ARRAY = TRUE;

-- ── Internal Stage ────────────────────────────────────────────────────────────
CREATE OR REPLACE STAGE UNIVERSITY_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT     = 'Internal stage for ETL file uploads';

-- ── Audit / Lineage table ─────────────────────────────────────────────────────
USE SCHEMA AUDIT;

CREATE TABLE IF NOT EXISTS ETL_RUN_LOG (
    run_id          VARCHAR(36)   DEFAULT UUID_STRING() PRIMARY KEY,
    pipeline_name   VARCHAR(100)  NOT NULL,
    source_system   VARCHAR(50),
    target_table    VARCHAR(100),
    rows_extracted  INTEGER       DEFAULT 0,
    rows_loaded     INTEGER       DEFAULT 0,
    rows_rejected   INTEGER       DEFAULT 0,
    status          VARCHAR(20)   DEFAULT 'RUNNING', -- RUNNING | SUCCESS | FAILED
    error_message   TEXT,
    started_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at    TIMESTAMP_NTZ,
    duration_secs   FLOAT GENERATED ALWAYS AS (
                        DATEDIFF('second', started_at, completed_at)
                    )
);

CREATE TABLE IF NOT EXISTS DATA_QUALITY_LOG (
    log_id          VARCHAR(36)   DEFAULT UUID_STRING() PRIMARY KEY,
    run_id          VARCHAR(36),
    table_name      VARCHAR(100),
    check_name      VARCHAR(100),
    check_result    VARCHAR(10),  -- PASS | FAIL | WARN
    expected_value  VARCHAR(200),
    actual_value    VARCHAR(200),
    row_count       INTEGER,
    checked_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
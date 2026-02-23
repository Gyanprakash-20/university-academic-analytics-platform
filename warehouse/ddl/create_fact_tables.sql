-- =============================================================================
-- FACT TABLES  —  Star Schema
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── FACT_ACADEMIC_PERFORMANCE  ─────────────────────────────────────────────────
-- Grain: one row per student × course × semester enrollment
CREATE TABLE IF NOT EXISTS FACT_ACADEMIC_PERFORMANCE (
    performance_key     INTEGER       AUTOINCREMENT PRIMARY KEY,
    -- Foreign keys (surrogate)
    student_key         INTEGER       NOT NULL REFERENCES DIM_STUDENT(student_key),
    course_key          INTEGER       NOT NULL REFERENCES DIM_COURSE(course_key),
    instructor_key      INTEGER       NOT NULL REFERENCES DIM_INSTRUCTOR(instructor_key),
    department_key      INTEGER       NOT NULL REFERENCES DIM_DEPARTMENT(department_key),
    enrollment_date_key INTEGER       NOT NULL REFERENCES DIM_DATE(date_key),
    grade_date_key      INTEGER               REFERENCES DIM_DATE(date_key),
    -- Degenerate dimension
    enrollment_id       VARCHAR(36)   NOT NULL,
    semester            VARCHAR(20),
    academic_year       VARCHAR(9),
    -- Measures
    grade_letter        VARCHAR(5),   -- A+ | A | A- | B+ … F | W | I
    grade_points        FLOAT,        -- 4.0 scale
    credit_hours        INTEGER,
    quality_points      FLOAT,        -- grade_points * credit_hours
    attendance_pct      FLOAT,        -- 0-100
    assignments_completed INTEGER,
    assignments_total   INTEGER,
    midterm_score       FLOAT,        -- 0-100
    final_score         FLOAT,        -- 0-100
    overall_score       FLOAT,        -- weighted average
    -- Derived / pre-aggregated
    is_passing          BOOLEAN       DEFAULT TRUE,
    is_withdrawn        BOOLEAN       DEFAULT FALSE,
    is_incomplete       BOOLEAN       DEFAULT FALSE,
    -- Metadata
    load_timestamp      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system       VARCHAR(50)   DEFAULT 'SIS',
    row_hash            VARCHAR(64),
    UNIQUE (enrollment_id)
)
CLUSTER BY (academic_year, semester, department_key)
COMMENT = 'Fact table: academic performance per enrollment — grain: student × course × semester'
DATA_RETENTION_TIME_IN_DAYS = 90;  -- Snowflake Time Travel

-- ── FACT_ATTENDANCE  ───────────────────────────────────────────────────────────
-- Grain: one row per student × course × date
CREATE TABLE IF NOT EXISTS FACT_ATTENDANCE (
    attendance_key      INTEGER       AUTOINCREMENT PRIMARY KEY,
    student_key         INTEGER       NOT NULL REFERENCES DIM_STUDENT(student_key),
    course_key          INTEGER       NOT NULL REFERENCES DIM_COURSE(course_key),
    instructor_key      INTEGER       NOT NULL REFERENCES DIM_INSTRUCTOR(instructor_key),
    department_key      INTEGER       NOT NULL REFERENCES DIM_DEPARTMENT(department_key),
    date_key            INTEGER       NOT NULL REFERENCES DIM_DATE(date_key),
    -- Measures
    status              VARCHAR(10)   NOT NULL,  -- PRESENT | ABSENT | LATE | EXCUSED
    is_present          BOOLEAN,
    is_excused          BOOLEAN       DEFAULT FALSE,
    minutes_late        INTEGER       DEFAULT 0,
    -- Metadata
    source_system       VARCHAR(20),
    load_timestamp      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (student_key, course_key, date_key)
)
CLUSTER BY (date_key, department_key)
COMMENT = 'Fact table: daily attendance — grain: student × course × date'
DATA_RETENTION_TIME_IN_DAYS = 90;

-- ── FACT_SEMESTER_GPA  (Aggregate fact) ────────────────────────────────────────
-- Grain: one row per student × semester (for fast dashboard queries)
CREATE TABLE IF NOT EXISTS FACT_SEMESTER_GPA (
    gpa_key             INTEGER       AUTOINCREMENT PRIMARY KEY,
    student_key         INTEGER       NOT NULL REFERENCES DIM_STUDENT(student_key),
    department_key      INTEGER       NOT NULL REFERENCES DIM_DEPARTMENT(department_key),
    academic_year       VARCHAR(9)    NOT NULL,
    semester            VARCHAR(20)   NOT NULL,
    -- Measures
    semester_gpa        FLOAT,
    cumulative_gpa      FLOAT,
    credit_hours_earned INTEGER,
    credit_hours_attempted INTEGER,
    courses_taken       INTEGER,
    courses_passed      INTEGER,
    avg_attendance_pct  FLOAT,
    -- Ranking (populated by Spark batch job)
    dept_gpa_rank       INTEGER,
    class_gpa_rank      INTEGER,
    -- Metadata
    calculated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (student_key, academic_year, semester)
)
CLUSTER BY (academic_year, semester)
COMMENT = 'Aggregate fact: GPA per student per semester';

-- ── INDEXES / SEARCH OPTIMIZATION  ────────────────────────────────────────────
-- Snowflake Search Optimization for selective point lookups
ALTER TABLE FACT_ACADEMIC_PERFORMANCE ADD SEARCH OPTIMIZATION ON EQUALITY(enrollment_id);
ALTER TABLE DIM_STUDENT              ADD SEARCH OPTIMIZATION ON EQUALITY(student_id);
ALTER TABLE DIM_COURSE               ADD SEARCH OPTIMIZATION ON EQUALITY(course_code);
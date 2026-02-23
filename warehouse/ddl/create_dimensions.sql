-- =============================================================================
-- DIMENSION TABLES  —  Star Schema
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── DIM_DATE  ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key        INTEGER       NOT NULL PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE          NOT NULL UNIQUE,
    day_of_week     INTEGER,        -- 1=Mon … 7=Sun
    day_name        VARCHAR(10),
    day_of_month    INTEGER,
    day_of_year     INTEGER,
    week_of_year    INTEGER,
    month_number    INTEGER,
    month_name      VARCHAR(10),
    quarter         INTEGER,
    year            INTEGER,
    academic_year   VARCHAR(9),     -- e.g. '2024-2025'
    semester        VARCHAR(20),    -- 'Fall 2024' | 'Spring 2025' | 'Summer 2025'
    is_weekday      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
)
CLUSTER BY (year, month_number)
COMMENT = 'Conformed date dimension — pre-populated for 2010-2035';

-- ── DIM_DEPARTMENT  ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_DEPARTMENT (
    department_key  INTEGER       AUTOINCREMENT PRIMARY KEY,
    department_id   VARCHAR(20)   NOT NULL UNIQUE,
    department_name VARCHAR(100)  NOT NULL,
    college         VARCHAR(100),
    faculty_count   INTEGER       DEFAULT 0,
    is_active       BOOLEAN       DEFAULT TRUE,
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Academic department dimension';

-- ── DIM_INSTRUCTOR  ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_INSTRUCTOR (
    instructor_key  INTEGER       AUTOINCREMENT PRIMARY KEY,
    instructor_id   VARCHAR(20)   NOT NULL UNIQUE,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    email           VARCHAR(100),
    rank            VARCHAR(50),   -- Professor | Associate | Assistant | Lecturer
    tenure_status   VARCHAR(20),   -- Tenured | Tenure-Track | Non-Tenure
    department_key  INTEGER        REFERENCES DIM_DEPARTMENT(department_key),
    hire_date       DATE,
    is_active       BOOLEAN        DEFAULT TRUE,
    created_at      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Instructor / faculty dimension';

-- ── DIM_COURSE  ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_COURSE (
    course_key      INTEGER       AUTOINCREMENT PRIMARY KEY,
    course_id       VARCHAR(20)   NOT NULL UNIQUE,
    course_code     VARCHAR(20),
    course_name     VARCHAR(200),
    credit_hours    INTEGER,
    level           VARCHAR(20),   -- Undergraduate | Graduate
    category        VARCHAR(50),   -- Core | Elective | Lab
    department_key  INTEGER        REFERENCES DIM_DEPARTMENT(department_key),
    is_active       BOOLEAN        DEFAULT TRUE,
    created_at      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Course catalog dimension';

-- ── DIM_STUDENT  (SCD Type 2)  ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_STUDENT (
    student_key         INTEGER       AUTOINCREMENT PRIMARY KEY,
    student_id          VARCHAR(20)   NOT NULL,
    -- SCD Type 2 columns
    effective_date      DATE          NOT NULL,
    expiry_date         DATE          DEFAULT '9999-12-31',
    is_current          BOOLEAN       DEFAULT TRUE,
    -- Slowly changing attributes
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    email               VARCHAR(100),  -- masked for non-privileged roles
    date_of_birth       DATE,          -- masked for non-privileged roles
    gender              VARCHAR(20),
    nationality         VARCHAR(50),
    -- Stable attributes
    enrollment_date     DATE,
    program             VARCHAR(100),
    major               VARCHAR(100),
    department_key      INTEGER        REFERENCES DIM_DEPARTMENT(department_key),
    advisor_key         INTEGER        REFERENCES DIM_INSTRUCTOR(instructor_key),
    student_type        VARCHAR(20),   -- Full-Time | Part-Time
    scholarship         BOOLEAN        DEFAULT FALSE,
    scholarship_amount  FLOAT          DEFAULT 0,
    -- Metadata
    created_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    -- SCD hash for change detection
    row_hash            VARCHAR(64)
)
CLUSTER BY (student_id, is_current)
COMMENT = 'Student dimension with SCD Type 2 change tracking';

-- ── STAGING TABLES  ────────────────────────────────────────────────────────────
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS STG_STUDENTS (
    raw_id          INTEGER       AUTOINCREMENT,
    student_id      VARCHAR(20),
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    email           VARCHAR(100),
    date_of_birth   VARCHAR(20),   -- raw string, parsed during transform
    gender          VARCHAR(20),
    nationality     VARCHAR(50),
    enrollment_date VARCHAR(20),
    program         VARCHAR(100),
    major           VARCHAR(100),
    department_id   VARCHAR(20),
    student_type    VARCHAR(20),
    scholarship     VARCHAR(10),
    scholarship_amount VARCHAR(20),
    source_file     VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_valid        BOOLEAN       DEFAULT TRUE,
    validation_errors TEXT
);

CREATE TABLE IF NOT EXISTS STG_COURSES (
    raw_id          INTEGER       AUTOINCREMENT,
    course_id       VARCHAR(20),
    course_code     VARCHAR(20),
    course_name     VARCHAR(200),
    credit_hours    VARCHAR(10),
    level           VARCHAR(20),
    category        VARCHAR(50),
    department_id   VARCHAR(20),
    source_file     VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STG_ENROLLMENTS (
    raw_id          INTEGER       AUTOINCREMENT,
    enrollment_id   VARCHAR(36),
    student_id      VARCHAR(20),
    course_id       VARCHAR(20),
    instructor_id   VARCHAR(20),
    semester        VARCHAR(20),
    academic_year   VARCHAR(9),
    enrollment_date VARCHAR(20),
    grade           VARCHAR(5),
    grade_points    VARCHAR(10),
    attendance_pct  VARCHAR(10),
    source_file     VARCHAR(200),
    load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STG_ATTENDANCE (
    raw_id          INTEGER       AUTOINCREMENT,
    student_id      VARCHAR(20),
    course_id       VARCHAR(20),
    attendance_date VARCHAR(20),
    status          VARCHAR(10),   -- PRESENT | ABSENT | LATE | EXCUSED
    source_system   VARCHAR(20),   -- LMS | RFID | MANUAL
    load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
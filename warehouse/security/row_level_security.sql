-- =============================================================================
-- SECURITY: ROW-LEVEL SECURITY (Row Access Policies)
-- Fine-grained access control at the data row level
-- Restricts what data each role can see without changing queries
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── 1. Setup: Role-to-Department mapping table ────────────────────────────────
-- Maintained by the data governance team; maps user roles to allowed departments
CREATE TABLE IF NOT EXISTS PROD.RLS_DEPARTMENT_ACCESS (
    role_name       VARCHAR(100)   NOT NULL,
    department_id   VARCHAR(20)    NOT NULL,
    granted_by      VARCHAR(100)   DEFAULT CURRENT_USER(),
    granted_at      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    is_active       BOOLEAN        DEFAULT TRUE,
    PRIMARY KEY (role_name, department_id)
);

-- Seed sample access grants
INSERT INTO PROD.RLS_DEPARTMENT_ACCESS (role_name, department_id) VALUES
    ('DEPT_CS_ADMIN',  'CS001'),
    ('DEPT_BA_ADMIN',  'BA001'),
    ('DEPT_ENG_ADMIN', 'CS001'),
    ('DEPT_ENG_ADMIN', 'EE001'),
    ('DEPT_ENG_ADMIN', 'ME001'),
    ('DEPT_SCI_ADMIN', 'MA001'),
    ('DEPT_SCI_ADMIN', 'PH001')
ON CONFLICT (role_name, department_id) DO NOTHING;

-- ── 2. Setup: Student-to-user mapping (for STUDENT_PORTAL role) ───────────────
CREATE TABLE IF NOT EXISTS PROD.RLS_STUDENT_USER_MAP (
    snowflake_username VARCHAR(100)   NOT NULL UNIQUE,
    student_id         VARCHAR(20)    NOT NULL,
    created_at         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ── 3. Row Access Policy: FACT_ACADEMIC_PERFORMANCE ──────────────────────────
-- Controls row visibility based on department_key and role
CREATE OR REPLACE ROW ACCESS POLICY RAP_FACT_PERFORMANCE
    AS (department_key INTEGER)
    RETURNS BOOLEAN ->
    CASE
        -- Full access roles see everything
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN', 'DATA_ENGINEER')
            THEN TRUE

        -- DATA_ANALYST and READONLY_DASHBOARD see all departments
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'READONLY_DASHBOARD')
            THEN TRUE

        -- DEPARTMENT_ADMIN: only their assigned departments
        WHEN CURRENT_ROLE() = 'DEPARTMENT_ADMIN'
            THEN EXISTS (
                SELECT 1
                FROM   PROD.RLS_DEPARTMENT_ACCESS rla
                JOIN   PROD.DIM_DEPARTMENT        d
                       ON  rla.department_id = d.department_id
                       AND d.department_key  = department_key
                WHERE  rla.role_name   = CURRENT_ROLE()
                  AND  rla.is_active   = TRUE
            )

        -- INSTRUCTOR role: only departments they teach in
        WHEN CURRENT_ROLE() = 'INSTRUCTOR'
            THEN EXISTS (
                SELECT 1
                FROM   PROD.DIM_INSTRUCTOR i
                WHERE  i.instructor_id = CURRENT_USER()   -- username = instructor_id convention
                  AND  i.department_key = department_key
                  AND  i.is_active      = TRUE
            )

        -- STUDENT_PORTAL: no row-level access here; handled at API layer
        -- Students access their own data through secure API endpoints
        WHEN CURRENT_ROLE() = 'STUDENT_PORTAL'
            THEN FALSE

        -- Default deny
        ELSE FALSE
    END;

-- Apply to fact tables
ALTER TABLE PROD.FACT_ACADEMIC_PERFORMANCE
    ADD ROW ACCESS POLICY RAP_FACT_PERFORMANCE ON (department_key);

ALTER TABLE PROD.FACT_ATTENDANCE
    ADD ROW ACCESS POLICY RAP_FACT_PERFORMANCE ON (department_key);

ALTER TABLE PROD.FACT_SEMESTER_GPA
    ADD ROW ACCESS POLICY RAP_FACT_PERFORMANCE ON (department_key);


-- ── 4. Row Access Policy: DIM_STUDENT ────────────────────────────────────────
-- Students should only see their own record via STUDENT_PORTAL role
CREATE OR REPLACE ROW ACCESS POLICY RAP_DIM_STUDENT
    AS (student_id VARCHAR)
    RETURNS BOOLEAN ->
    CASE
        -- Full access
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN', 'DATA_ENGINEER',
                                 'DATA_ANALYST', 'READONLY_DASHBOARD')
            THEN TRUE

        -- Department admins see students in their departments
        WHEN CURRENT_ROLE() = 'DEPARTMENT_ADMIN'
            THEN EXISTS (
                SELECT 1
                FROM   PROD.RLS_DEPARTMENT_ACCESS rla
                JOIN   PROD.DIM_STUDENT           s
                       ON  s.department_key IN (
                           SELECT d.department_key
                           FROM   PROD.DIM_DEPARTMENT d
                           WHERE  d.department_id = rla.department_id
                       )
                       AND s.student_id = student_id
                WHERE  rla.role_name = CURRENT_ROLE()
                  AND  rla.is_active = TRUE
            )

        -- Students see only their own record
        WHEN CURRENT_ROLE() = 'STUDENT_PORTAL'
            THEN EXISTS (
                SELECT 1
                FROM   PROD.RLS_STUDENT_USER_MAP m
                WHERE  m.snowflake_username = CURRENT_USER()
                  AND  m.student_id         = student_id
            )

        -- Instructors see students enrolled in their courses (simplified)
        WHEN CURRENT_ROLE() = 'INSTRUCTOR'
            THEN EXISTS (
                SELECT 1
                FROM   PROD.FACT_ACADEMIC_PERFORMANCE f
                JOIN   PROD.DIM_STUDENT               s ON f.student_key = s.student_key
                JOIN   PROD.DIM_INSTRUCTOR             i ON f.instructor_key = i.instructor_key
                WHERE  s.student_id     = student_id
                  AND  i.instructor_id  = CURRENT_USER()
            )

        ELSE FALSE
    END;

ALTER TABLE PROD.DIM_STUDENT
    ADD ROW ACCESS POLICY RAP_DIM_STUDENT ON (student_id);


-- ── 5. Audit Table: Track who accessed which rows ─────────────────────────────
-- Note: Snowflake Enterprise provides SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
-- This is a supplementary application-level audit log
CREATE TABLE IF NOT EXISTS AUDIT.ROW_ACCESS_LOG (
    log_id          VARCHAR(36)    DEFAULT UUID_STRING() PRIMARY KEY,
    session_user    VARCHAR(100)   DEFAULT CURRENT_USER(),
    session_role    VARCHAR(100)   DEFAULT CURRENT_ROLE(),
    table_name      VARCHAR(100),
    filter_context  VARCHAR(500),  -- what filter was applied
    row_count       INTEGER,
    accessed_at     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ── 6. Object Tagging for Data Classification ─────────────────────────────────
-- Tag PII columns for governance reporting
CREATE TAG IF NOT EXISTS PROD.PII_TAG
    ALLOWED_VALUES 'HIGH', 'MEDIUM', 'LOW'
    COMMENT = 'PII sensitivity classification tag';

ALTER TABLE PROD.DIM_STUDENT MODIFY COLUMN email
    SET TAG PROD.PII_TAG = 'HIGH';

ALTER TABLE PROD.DIM_STUDENT MODIFY COLUMN date_of_birth
    SET TAG PROD.PII_TAG = 'HIGH';

ALTER TABLE PROD.DIM_STUDENT MODIFY COLUMN first_name
    SET TAG PROD.PII_TAG = 'MEDIUM';

ALTER TABLE PROD.DIM_STUDENT MODIFY COLUMN last_name
    SET TAG PROD.PII_TAG = 'MEDIUM';

ALTER TABLE PROD.DIM_STUDENT MODIFY COLUMN scholarship_amount
    SET TAG PROD.PII_TAG = 'HIGH';

-- Query to find all tagged PII columns:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
--     'UNIVERSITY_DW.PROD.DIM_STUDENT', 'table'
-- ));


-- ── 7. Network Policy (restrict access by IP range) ───────────────────────────
CREATE NETWORK POLICY IF NOT EXISTS UNIVERSITY_CAMPUS_POLICY
    ALLOWED_IP_LIST   = ('10.0.0.0/8', '192.168.0.0/16', '172.16.0.0/12')
    BLOCKED_IP_LIST   = ()
    COMMENT           = 'Restrict DW access to campus and VPN IP ranges';

-- Apply to service accounts (not full admins):
-- ALTER USER etl_service_account SET NETWORK_POLICY = UNIVERSITY_CAMPUS_POLICY;
-- ALTER USER dashboard_service   SET NETWORK_POLICY = UNIVERSITY_CAMPUS_POLICY;
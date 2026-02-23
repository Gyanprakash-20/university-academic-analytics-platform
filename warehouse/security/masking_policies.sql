-- =============================================================================
-- SECURITY: DYNAMIC DATA MASKING + ROW-LEVEL SECURITY
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── 1. Masking Policies ────────────────────────────────────────────────────────

-- Email: only UNIVERSITY_ADMIN / DATA_ENGINEER see full email
CREATE OR REPLACE MASKING POLICY MASK_EMAIL AS (email_val VARCHAR)
    RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN', 'DATA_ENGINEER') THEN email_val
        ELSE REGEXP_REPLACE(email_val, '(.{2}).+(@.+)', '\\1***\\2')
        -- e.g.  jo***@example.edu
    END;

-- Date of birth: full date for admin, birth year only for analysts
CREATE OR REPLACE MASKING POLICY MASK_DOB AS (dob DATE)
    RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN', 'DATA_ENGINEER') THEN dob
        ELSE DATE_FROM_PARTS(YEAR(dob), 1, 1)  -- return Jan 1 of birth year
    END;

-- Student name: fully masked for student portal (they see their own via RLS)
CREATE OR REPLACE MASKING POLICY MASK_STUDENT_NAME AS (name_val VARCHAR)
    RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN', 'DATA_ENGINEER', 'DATA_ANALYST') THEN name_val
        WHEN CURRENT_ROLE() IN ('DEPARTMENT_ADMIN', 'INSTRUCTOR') THEN name_val
        ELSE '***'  -- student portal sees masked names (own record via RLS)
    END;

-- Scholarship amount: masked for non-admin roles
CREATE OR REPLACE MASKING POLICY MASK_SCHOLARSHIP_AMOUNT AS (amount FLOAT)
    RETURNS FLOAT ->
    CASE
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN', 'DATA_ENGINEER') THEN amount
        ELSE -1  -- sentinel value indicating masked
    END;

-- Generic full-mask (for internal IDs visible to admins only)
CREATE OR REPLACE MASKING POLICY MASK_FULL AS (val VARCHAR)
    RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN') THEN val
        ELSE '***REDACTED***'
    END;

-- ── 2. Apply Masking Policies to DIM_STUDENT columns ─────────────────────────
ALTER TABLE DIM_STUDENT MODIFY COLUMN email
    SET MASKING POLICY MASK_EMAIL;

ALTER TABLE DIM_STUDENT MODIFY COLUMN date_of_birth
    SET MASKING POLICY MASK_DOB;

ALTER TABLE DIM_STUDENT MODIFY COLUMN first_name
    SET MASKING POLICY MASK_STUDENT_NAME;

ALTER TABLE DIM_STUDENT MODIFY COLUMN last_name
    SET MASKING POLICY MASK_STUDENT_NAME;

ALTER TABLE DIM_STUDENT MODIFY COLUMN scholarship_amount
    SET MASKING POLICY MASK_SCHOLARSHIP_AMOUNT;

-- ── 3. Row-Level Security (Row Access Policies) ────────────────────────────────

-- Mapping table: which roles can see which departments
CREATE TABLE IF NOT EXISTS RLS_DEPARTMENT_ACCESS (
    role_name       VARCHAR(100),
    department_id   VARCHAR(20),
    PRIMARY KEY (role_name, department_id)
);

-- Row access policy for FACT_ACADEMIC_PERFORMANCE
CREATE OR REPLACE ROW ACCESS POLICY RAP_PERFORMANCE AS (department_key INTEGER)
    RETURNS BOOLEAN ->
    CASE
        -- Admins and data engineers see everything
        WHEN CURRENT_ROLE() IN ('UNIVERSITY_ADMIN', 'DATA_ENGINEER', 'DATA_ANALYST') THEN TRUE
        -- Department admins see only their department
        WHEN CURRENT_ROLE() = 'DEPARTMENT_ADMIN' THEN EXISTS (
            SELECT 1
            FROM   RLS_DEPARTMENT_ACCESS rls
            JOIN   DIM_DEPARTMENT        d  ON rls.department_id = d.department_id
            WHERE  rls.role_name   = CURRENT_ROLE()
              AND  d.department_key = department_key
        )
        -- Student portal: students see their own record (handled at API layer)
        WHEN CURRENT_ROLE() = 'STUDENT_PORTAL' THEN FALSE  -- blocked here; API filters
        -- Dashboard service: aggregates only, no row-level
        WHEN CURRENT_ROLE() = 'READONLY_DASHBOARD' THEN TRUE
        ELSE FALSE
    END;

-- Apply the row access policy
ALTER TABLE FACT_ACADEMIC_PERFORMANCE
    ADD ROW ACCESS POLICY RAP_PERFORMANCE ON (department_key);

ALTER TABLE FACT_ATTENDANCE
    ADD ROW ACCESS POLICY RAP_PERFORMANCE ON (department_key);

-- ── 4. Audit policy: log all SELECT on PII tables ─────────────────────────────
-- (Snowflake Enterprise: use ACCESS_HISTORY view for audit)
-- Grant access to governance role:
CREATE ROLE IF NOT EXISTS DATA_GOVERNANCE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DATA_GOVERNANCE;
-- DATA_GOVERNANCE can then query SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
-- =============================================================================
-- TRANSFORMATION: FACT_ACADEMIC_PERFORMANCE
-- Incremental load from STAGING → PROD using MERGE
-- Includes derived metrics, grade enrichment, and data quality checks
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── Step 1: Pre-validation — flag bad staging records ─────────────────────────
CREATE OR REPLACE TEMPORARY TABLE STAGING.STG_ENROLLMENTS_VALIDATED AS
SELECT
    e.*,
    -- Validation flags
    (e.student_id IS NOT NULL)                              AS v_has_student_id,
    (e.course_id  IS NOT NULL)                              AS v_has_course_id,
    (e.grade IN ('A+','A','A-','B+','B','B-','C+','C','C-',
                 'D+','D','D-','F','W','I','AU'))            AS v_valid_grade,
    (TRY_TO_DATE(e.enrollment_date, 'YYYY-MM-DD') IS NOT NULL) AS v_valid_enroll_date,
    (TRY_TO_DECIMAL(e.attendance_pct, 5, 2)
        BETWEEN 0 AND 100)                                  AS v_valid_attendance,
    -- Computed grade points
    CASE e.grade
        WHEN 'A+' THEN 4.0  WHEN 'A'  THEN 4.0  WHEN 'A-' THEN 3.7
        WHEN 'B+' THEN 3.3  WHEN 'B'  THEN 3.0  WHEN 'B-' THEN 2.7
        WHEN 'C+' THEN 2.3  WHEN 'C'  THEN 2.0  WHEN 'C-' THEN 1.7
        WHEN 'D+' THEN 1.3  WHEN 'D'  THEN 1.0  WHEN 'D-' THEN 0.7
        WHEN 'F'  THEN 0.0  ELSE NULL
    END                                                     AS computed_grade_points,
    -- Is GPA-eligible (not W, I, AU)
    (e.grade NOT IN ('W', 'I', 'AU'))                       AS is_gpa_eligible
FROM STAGING.STG_ENROLLMENTS e;

-- ── Step 2: Log validation summary to AUDIT ───────────────────────────────────
INSERT INTO AUDIT.DATA_QUALITY_LOG (
    table_name, check_name, check_result, expected_value, actual_value, row_count
)
SELECT
    'STG_ENROLLMENTS',
    'valid_grades',
    CASE WHEN SUM(CASE WHEN NOT v_valid_grade THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    '0 invalid grades',
    SUM(CASE WHEN NOT v_valid_grade THEN 1 ELSE 0 END)::VARCHAR || ' invalid grades',
    COUNT(*)
FROM STAGING.STG_ENROLLMENTS_VALIDATED;

INSERT INTO AUDIT.DATA_QUALITY_LOG (
    table_name, check_name, check_result, expected_value, actual_value, row_count
)
SELECT
    'STG_ENROLLMENTS',
    'valid_attendance_range',
    CASE WHEN SUM(CASE WHEN NOT v_valid_attendance THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    'attendance_pct between 0-100',
    SUM(CASE WHEN NOT v_valid_attendance THEN 1 ELSE 0 END)::VARCHAR || ' out-of-range',
    COUNT(*)
FROM STAGING.STG_ENROLLMENTS_VALIDATED;

-- ── Step 3: MERGE into FACT_ACADEMIC_PERFORMANCE ──────────────────────────────
MERGE INTO PROD.FACT_ACADEMIC_PERFORMANCE  tgt
USING (
    SELECT
        -- Surrogate key lookups
        s.student_key,
        c.course_key,
        COALESCE(i.instructor_key, 1)                       AS instructor_key,
        s.department_key,
        -- Date keys
        TO_NUMBER(TO_CHAR(
            TRY_TO_DATE(e.enrollment_date, 'YYYY-MM-DD'), 'YYYYMMDD'
        ))                                                  AS enrollment_date_key,
        NULL::INTEGER                                       AS grade_date_key,
        -- Degenerate dimensions
        e.enrollment_id,
        e.semester,
        e.academic_year,
        -- Grade measures
        e.grade                                             AS grade_letter,
        e.computed_grade_points                             AS grade_points,
        COALESCE(c_dim.credit_hours, 3)                    AS credit_hours,
        COALESCE(e.computed_grade_points, 0)
            * COALESCE(c_dim.credit_hours, 3)              AS quality_points,
        COALESCE(TRY_TO_DECIMAL(e.attendance_pct, 5, 2), 0) AS attendance_pct,
        COALESCE(e.assignments_completed::INTEGER, 0)      AS assignments_completed,
        COALESCE(e.assignments_total::INTEGER, 12)         AS assignments_total,
        COALESCE(TRY_TO_DECIMAL(e.midterm_score, 5, 2), 0) AS midterm_score,
        COALESCE(TRY_TO_DECIMAL(e.final_score,   5, 2), 0) AS final_score,
        -- Weighted overall score: midterm 35% + final 45% + attendance 20%
        ROUND(
            COALESCE(TRY_TO_DECIMAL(e.midterm_score, 5, 2), 0) * 0.35 +
            COALESCE(TRY_TO_DECIMAL(e.final_score,   5, 2), 0) * 0.45 +
            COALESCE(TRY_TO_DECIMAL(e.attendance_pct,5, 2), 0) * 0.20,
            2
        )                                                   AS overall_score,
        -- Boolean flags
        (e.grade NOT IN ('F', 'W', 'I') AND e.grade IS NOT NULL) AS is_passing,
        (e.grade = 'W')                                     AS is_withdrawn,
        (e.grade = 'I')                                     AS is_incomplete,
        -- Row hash for change detection
        MD5(
            COALESCE(e.enrollment_id, '') || '|' ||
            COALESCE(e.grade,         '') || '|' ||
            COALESCE(e.attendance_pct,'') || '|' ||
            COALESCE(e.final_score,   '')
        )                                                   AS row_hash
    FROM STAGING.STG_ENROLLMENTS_VALIDATED e
    -- Only process valid records
    JOIN PROD.DIM_STUDENT    s    ON e.student_id    = s.student_id
                                 AND s.is_current    = TRUE
    JOIN PROD.DIM_COURSE     c    ON e.course_id     = c.course_id
    JOIN PROD.DIM_COURSE     c_dim ON e.course_id    = c_dim.course_id
    LEFT JOIN PROD.DIM_INSTRUCTOR i ON e.instructor_id = i.instructor_id
    WHERE e.v_has_student_id = TRUE
      AND e.v_has_course_id  = TRUE
      AND e.v_valid_grade    = TRUE
) src
ON tgt.enrollment_id = src.enrollment_id

-- Update changed records
WHEN MATCHED AND tgt.row_hash != src.row_hash THEN UPDATE SET
    tgt.grade_letter          = src.grade_letter,
    tgt.grade_points          = src.grade_points,
    tgt.quality_points        = src.quality_points,
    tgt.attendance_pct        = src.attendance_pct,
    tgt.overall_score         = src.overall_score,
    tgt.midterm_score         = src.midterm_score,
    tgt.final_score           = src.final_score,
    tgt.is_passing            = src.is_passing,
    tgt.is_withdrawn          = src.is_withdrawn,
    tgt.is_incomplete         = src.is_incomplete,
    tgt.row_hash              = src.row_hash,
    tgt.load_timestamp        = CURRENT_TIMESTAMP()

-- Insert new records
WHEN NOT MATCHED THEN INSERT (
    student_key, course_key, instructor_key, department_key,
    enrollment_date_key, grade_date_key,
    enrollment_id, semester, academic_year,
    grade_letter, grade_points, credit_hours, quality_points,
    attendance_pct, assignments_completed, assignments_total,
    midterm_score, final_score, overall_score,
    is_passing, is_withdrawn, is_incomplete,
    source_system, row_hash
) VALUES (
    src.student_key, src.course_key, src.instructor_key, src.department_key,
    src.enrollment_date_key, src.grade_date_key,
    src.enrollment_id, src.semester, src.academic_year,
    src.grade_letter, src.grade_points, src.credit_hours, src.quality_points,
    src.attendance_pct, src.assignments_completed, src.assignments_total,
    src.midterm_score, src.final_score, src.overall_score,
    src.is_passing, src.is_withdrawn, src.is_incomplete,
    'SIS', src.row_hash
);

-- ── Step 4: Post-load row count audit ─────────────────────────────────────────
INSERT INTO AUDIT.DATA_QUALITY_LOG (
    table_name, check_name, check_result, expected_value, actual_value, row_count
)
SELECT
    'FACT_ACADEMIC_PERFORMANCE',
    'post_load_row_count',
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    '> 0 rows',
    COUNT(*)::VARCHAR || ' total rows',
    COUNT(*)
FROM PROD.FACT_ACADEMIC_PERFORMANCE;

-- ── Step 5: Refresh aggregate GPA fact ────────────────────────────────────────
INSERT OVERWRITE INTO PROD.FACT_SEMESTER_GPA (
    student_key, department_key, academic_year, semester,
    semester_gpa, credit_hours_earned, credit_hours_attempted,
    courses_taken, courses_passed, avg_attendance_pct
)
SELECT
    f.student_key,
    f.department_key,
    f.academic_year,
    f.semester,
    ROUND(
        SUM(CASE WHEN f.is_gpa_eligible THEN f.quality_points ELSE 0 END) /
        NULLIF(SUM(CASE WHEN f.is_gpa_eligible THEN f.credit_hours ELSE 0 END), 0),
        3
    )                                              AS semester_gpa,
    SUM(CASE WHEN f.is_passing  THEN f.credit_hours ELSE 0 END) AS credit_hours_earned,
    SUM(CASE WHEN f.grade_points IS NOT NULL THEN f.credit_hours ELSE 0 END) AS credit_hours_attempted,
    COUNT(*)                                       AS courses_taken,
    SUM(f.is_passing::INTEGER)                     AS courses_passed,
    ROUND(AVG(f.attendance_pct), 2)               AS avg_attendance_pct
FROM PROD.FACT_ACADEMIC_PERFORMANCE f
WHERE f.is_withdrawn  = FALSE
  AND f.is_incomplete = FALSE
GROUP BY f.student_key, f.department_key, f.academic_year, f.semester;
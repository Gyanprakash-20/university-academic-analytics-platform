-- =============================================================================
-- TRANSFORMATION: FACT_ATTENDANCE
-- Loads and validates daily attendance records from STAGING → PROD
-- Supports both batch (historical) and micro-batch (streaming) loads
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── Step 1: Validate staging attendance records ────────────────────────────────
CREATE OR REPLACE TEMPORARY TABLE STAGING.STG_ATTENDANCE_VALIDATED AS
SELECT
    a.*,
    -- Validation checks
    (a.student_id IS NOT NULL)                              AS v_has_student_id,
    (a.course_id  IS NOT NULL)                              AS v_has_course_id,
    (TRY_TO_DATE(a.attendance_date, 'YYYY-MM-DD') IS NOT NULL) AS v_valid_date,
    (a.status IN ('PRESENT','ABSENT','LATE','EXCUSED'))     AS v_valid_status,
    -- Parsed date
    TRY_TO_DATE(a.attendance_date, 'YYYY-MM-DD')           AS parsed_date,
    -- Is present (PRESENT or LATE counts as attending)
    (a.status IN ('PRESENT', 'LATE'))                       AS computed_is_present,
    (a.status = 'EXCUSED')                                  AS computed_is_excused,
    COALESCE(a.minutes_late::INTEGER, 0)                   AS computed_minutes_late,
    -- Date dimension key
    TO_NUMBER(TO_CHAR(
        TRY_TO_DATE(a.attendance_date, 'YYYY-MM-DD'), 'YYYYMMDD'
    ))                                                      AS date_key
FROM STAGING.STG_ATTENDANCE a;

-- ── Step 2: Log data quality results ──────────────────────────────────────────
INSERT INTO AUDIT.DATA_QUALITY_LOG (
    table_name, check_name, check_result, expected_value, actual_value, row_count
)
SELECT
    'STG_ATTENDANCE',
    'valid_status_values',
    CASE WHEN SUM(CASE WHEN NOT v_valid_status THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    'All statuses in {PRESENT,ABSENT,LATE,EXCUSED}',
    SUM(CASE WHEN NOT v_valid_status THEN 1 ELSE 0 END)::VARCHAR || ' invalid statuses',
    COUNT(*)
FROM STAGING.STG_ATTENDANCE_VALIDATED;

INSERT INTO AUDIT.DATA_QUALITY_LOG (
    table_name, check_name, check_result, expected_value, actual_value, row_count
)
SELECT
    'STG_ATTENDANCE',
    'valid_attendance_dates',
    CASE WHEN SUM(CASE WHEN NOT v_valid_date THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    'All dates parseable as YYYY-MM-DD',
    SUM(CASE WHEN NOT v_valid_date THEN 1 ELSE 0 END)::VARCHAR || ' invalid dates',
    COUNT(*)
FROM STAGING.STG_ATTENDANCE_VALIDATED;

-- ── Step 3: MERGE into FACT_ATTENDANCE ────────────────────────────────────────
MERGE INTO PROD.FACT_ATTENDANCE  tgt
USING (
    SELECT
        s.student_key,
        c.course_key,
        -- Default instructor key 1 if not captured
        COALESCE(
            (SELECT instructor_key FROM PROD.DIM_INSTRUCTOR
             WHERE is_active = TRUE LIMIT 1), 1
        )                                   AS instructor_key,
        s.department_key,
        a.date_key,
        a.status,
        a.computed_is_present               AS is_present,
        a.computed_is_excused               AS is_excused,
        a.computed_minutes_late             AS minutes_late,
        COALESCE(a.source_system, 'UNKNOWN') AS source_system
    FROM STAGING.STG_ATTENDANCE_VALIDATED  a
    JOIN PROD.DIM_STUDENT     s   ON a.student_id  = s.student_id
                                 AND s.is_current  = TRUE
    JOIN PROD.DIM_COURSE      c   ON a.course_id   = c.course_id
    JOIN PROD.DIM_DATE        dt  ON a.date_key    = dt.date_key
    WHERE a.v_has_student_id = TRUE
      AND a.v_has_course_id  = TRUE
      AND a.v_valid_date     = TRUE
      AND a.v_valid_status   = TRUE
      -- Only load weekday attendance (courses don't run on weekends)
      AND dt.is_weekday      = TRUE
) src
ON  tgt.student_key = src.student_key
AND tgt.course_key  = src.course_key
AND tgt.date_key    = src.date_key

-- Update if status changed (e.g. ABSENT → EXCUSED after appeal)
WHEN MATCHED AND tgt.status != src.status THEN UPDATE SET
    tgt.status          = src.status,
    tgt.is_present      = src.is_present,
    tgt.is_excused      = src.is_excused,
    tgt.minutes_late    = src.minutes_late,
    tgt.load_timestamp  = CURRENT_TIMESTAMP()

-- Insert new attendance records
WHEN NOT MATCHED THEN INSERT (
    student_key, course_key, instructor_key, department_key, date_key,
    status, is_present, is_excused, minutes_late, source_system
) VALUES (
    src.student_key, src.course_key, src.instructor_key, src.department_key, src.date_key,
    src.status, src.is_present, src.is_excused, src.minutes_late, src.source_system
);

-- ── Step 4: Back-propagate attendance % into FACT_ACADEMIC_PERFORMANCE ────────
-- Update attendance_pct in the performance fact from actual attendance records
UPDATE PROD.FACT_ACADEMIC_PERFORMANCE f
SET
    f.attendance_pct = att_summary.actual_pct,
    f.load_timestamp  = CURRENT_TIMESTAMP()
FROM (
    SELECT
        fa.student_key,
        fa.course_key,
        dd.academic_year,
        dd.semester,
        ROUND(
            SUM(fa.is_present::INTEGER) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ) AS actual_pct
    FROM PROD.FACT_ATTENDANCE fa
    JOIN PROD.DIM_DATE        dd ON fa.date_key = dd.date_key
    GROUP BY fa.student_key, fa.course_key, dd.academic_year, dd.semester
) att_summary
WHERE f.student_key   = att_summary.student_key
  AND f.course_key    = att_summary.course_key
  AND f.academic_year = att_summary.academic_year
  AND f.semester      = att_summary.semester;

-- ── Step 5: Build real-time attendance summary view ───────────────────────────
CREATE OR REPLACE VIEW PROD.V_ATTENDANCE_SUMMARY AS
SELECT
    d.department_name,
    dd.academic_year,
    dd.semester,
    dd.month_name,
    COUNT(*)                                               AS total_records,
    SUM(fa.is_present::INTEGER)                            AS present_count,
    SUM(CASE WHEN fa.status = 'ABSENT'  THEN 1 ELSE 0 END) AS absent_count,
    SUM(CASE WHEN fa.status = 'LATE'    THEN 1 ELSE 0 END) AS late_count,
    SUM(fa.is_excused::INTEGER)                            AS excused_count,
    ROUND(
        SUM(fa.is_present::INTEGER) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                      AS attendance_rate_pct,
    ROUND(AVG(fa.minutes_late), 1)                        AS avg_minutes_late,
    COUNT(DISTINCT fa.student_key)                         AS unique_students,
    COUNT(DISTINCT fa.course_key)                          AS unique_courses
FROM PROD.FACT_ATTENDANCE   fa
JOIN PROD.DIM_DEPARTMENT    d   ON fa.department_key = d.department_key
JOIN PROD.DIM_DATE          dd  ON fa.date_key       = dd.date_key
GROUP BY d.department_name, dd.academic_year, dd.semester, dd.month_name;

-- ── Step 6: Audit post-load ────────────────────────────────────────────────────
INSERT INTO AUDIT.DATA_QUALITY_LOG (
    table_name, check_name, check_result, expected_value, actual_value, row_count
)
SELECT
    'FACT_ATTENDANCE',
    'post_load_completeness',
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
    '> 0 attendance records loaded',
    COUNT(*)::VARCHAR || ' attendance records',
    COUNT(*)
FROM PROD.FACT_ATTENDANCE;
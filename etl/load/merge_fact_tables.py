"""
Load: Merge into Production Fact Tables (Snowflake MERGE / UPSERT)
===================================================================
Implements SCD Type 2 for DIM_STUDENT and MERGE INTO for fact tables.
"""
from __future__ import annotations

from config.logging_config import logger
from config.snowflake_config import execute_statement, execute_query


# ── SCD Type 2 MERGE for DIM_STUDENT ──────────────────────────────────────────
MERGE_DIM_STUDENT = """
MERGE INTO PROD.DIM_STUDENT  tgt
USING (
    SELECT
        s.student_id,
        s.first_name,
        s.last_name,
        s.email,
        TRY_TO_DATE(s.date_of_birth,   'YYYY-MM-DD') AS date_of_birth,
        s.gender,
        s.nationality,
        TRY_TO_DATE(s.enrollment_date, 'YYYY-MM-DD') AS enrollment_date,
        s.program,
        s.major,
        d.department_key,
        s.student_type,
        s.scholarship::BOOLEAN                         AS scholarship,
        s.scholarship_amount::FLOAT                    AS scholarship_amount,
        s.row_hash,
        CURRENT_DATE                                   AS effective_date
    FROM STAGING.STG_STUDENTS s
    JOIN PROD.DIM_DEPARTMENT   d ON s.department_id = d.department_id
    WHERE s.is_valid = TRUE
) src
ON  tgt.student_id = src.student_id
AND tgt.is_current = TRUE

-- Existing record, attributes changed → close old row
WHEN MATCHED AND tgt.row_hash != src.row_hash THEN UPDATE SET
    tgt.is_current   = FALSE,
    tgt.expiry_date  = src.effective_date,
    tgt.updated_at   = CURRENT_TIMESTAMP()

-- Existing record, no change → do nothing (WHEN MATCHED without action inserts nothing)
;

-- Insert new records (new students OR new version of changed students)
INSERT INTO PROD.DIM_STUDENT (
    student_id, first_name, last_name, email, date_of_birth,
    gender, nationality, enrollment_date, program, major,
    department_key, student_type, scholarship, scholarship_amount,
    effective_date, expiry_date, is_current, row_hash
)
SELECT
    src.student_id, src.first_name, src.last_name, src.email, src.date_of_birth,
    src.gender, src.nationality, src.enrollment_date, src.program, src.major,
    src.department_key, src.student_type, src.scholarship, src.scholarship_amount,
    src.effective_date, '9999-12-31', TRUE, src.row_hash
FROM (
    SELECT
        s.student_id,
        s.first_name, s.last_name, s.email,
        TRY_TO_DATE(s.date_of_birth, 'YYYY-MM-DD') AS date_of_birth,
        s.gender, s.nationality,
        TRY_TO_DATE(s.enrollment_date, 'YYYY-MM-DD') AS enrollment_date,
        s.program, s.major, d.department_key,
        s.student_type, s.scholarship::BOOLEAN AS scholarship,
        s.scholarship_amount::FLOAT AS scholarship_amount,
        s.row_hash,
        CURRENT_DATE AS effective_date
    FROM STAGING.STG_STUDENTS s
    JOIN PROD.DIM_DEPARTMENT   d ON s.department_id = d.department_id
    WHERE s.is_valid = TRUE
) src
WHERE NOT EXISTS (
    SELECT 1 FROM PROD.DIM_STUDENT t
    WHERE t.student_id = src.student_id
      AND t.row_hash   = src.row_hash
      AND t.is_current = TRUE
);
"""


# ── MERGE for FACT_ACADEMIC_PERFORMANCE ───────────────────────────────────────
MERGE_FACT_PERFORMANCE = """
MERGE INTO PROD.FACT_ACADEMIC_PERFORMANCE  tgt
USING (
    SELECT
        s.student_key,
        c.course_key,
        COALESCE(i.instructor_key, 1)      AS instructor_key,
        d.department_key,
        TO_NUMBER(TO_CHAR(
            TRY_TO_DATE(e.enrollment_date, 'YYYY-MM-DD'), 'YYYYMMDD'
        ))                                  AS enrollment_date_key,
        e.enrollment_id,
        e.semester,
        e.academic_year,
        e.grade                             AS grade_letter,
        e.grade_points::FLOAT               AS grade_points,
        c_dim.credit_hours,
        (e.grade_points::FLOAT * c_dim.credit_hours) AS quality_points,
        e.attendance_pct::FLOAT             AS attendance_pct,
        e.assignments_completed::INTEGER    AS assignments_completed,
        e.assignments_total::INTEGER        AS assignments_total,
        e.midterm_score::FLOAT              AS midterm_score,
        e.final_score::FLOAT                AS final_score,
        (e.midterm_score::FLOAT * 0.35 +
         e.final_score::FLOAT  * 0.45 +
         e.attendance_pct::FLOAT * 0.20)   AS overall_score,
        (e.grade NOT IN ('F','W','I'))      AS is_passing,
        (e.grade = 'W')                     AS is_withdrawn,
        (e.grade = 'I')                     AS is_incomplete,
        MD5(e.enrollment_id || e.grade || e.attendance_pct) AS row_hash
    FROM STAGING.STG_ENROLLMENTS           e
    JOIN PROD.DIM_STUDENT                  s ON e.student_id    = s.student_id
                                             AND s.is_current   = TRUE
    JOIN PROD.DIM_COURSE                   c ON e.course_id     = c.course_id
    JOIN PROD.DIM_COURSE                c_dim ON e.course_id    = c_dim.course_id
    JOIN PROD.DIM_DEPARTMENT               d ON s.department_key = d.department_key
    LEFT JOIN PROD.DIM_INSTRUCTOR          i ON e.instructor_id = i.instructor_id
) src
ON tgt.enrollment_id = src.enrollment_id

WHEN MATCHED AND tgt.row_hash != src.row_hash THEN UPDATE SET
    tgt.grade_letter   = src.grade_letter,
    tgt.grade_points   = src.grade_points,
    tgt.quality_points = src.quality_points,
    tgt.attendance_pct = src.attendance_pct,
    tgt.overall_score  = src.overall_score,
    tgt.is_passing     = src.is_passing,
    tgt.is_withdrawn   = src.is_withdrawn,
    tgt.is_incomplete  = src.is_incomplete,
    tgt.row_hash       = src.row_hash,
    tgt.load_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    student_key, course_key, instructor_key, department_key,
    enrollment_date_key, enrollment_id, semester, academic_year,
    grade_letter, grade_points, credit_hours, quality_points,
    attendance_pct, assignments_completed, assignments_total,
    midterm_score, final_score, overall_score,
    is_passing, is_withdrawn, is_incomplete, row_hash
) VALUES (
    src.student_key, src.course_key, src.instructor_key, src.department_key,
    src.enrollment_date_key, src.enrollment_id, src.semester, src.academic_year,
    src.grade_letter, src.grade_points, src.credit_hours, src.quality_points,
    src.attendance_pct, src.assignments_completed, src.assignments_total,
    src.midterm_score, src.final_score, src.overall_score,
    src.is_passing, src.is_withdrawn, src.is_incomplete, src.row_hash
);
"""


# ── Public API ─────────────────────────────────────────────────────────────────
def merge_dim_student() -> None:
    logger.info("Merging DIM_STUDENT (SCD Type 2) …")
    execute_statement(MERGE_DIM_STUDENT)
    count = execute_query("SELECT COUNT(*) AS cnt FROM PROD.DIM_STUDENT WHERE is_current = TRUE")[0]["CNT"]
    logger.info(f"DIM_STUDENT current records: {count:,}")


def merge_fact_performance() -> None:
    logger.info("Merging FACT_ACADEMIC_PERFORMANCE …")
    execute_statement(MERGE_FACT_PERFORMANCE)
    count = execute_query("SELECT COUNT(*) AS cnt FROM PROD.FACT_ACADEMIC_PERFORMANCE")[0]["CNT"]
    logger.info(f"FACT_ACADEMIC_PERFORMANCE total records: {count:,}")


def merge_fact_attendance() -> None:
    logger.info("Merging FACT_ATTENDANCE …")
    sql = """
    MERGE INTO PROD.FACT_ATTENDANCE tgt
    USING (
        SELECT
            s.student_key,
            c.course_key,
            COALESCE(1, 1)              AS instructor_key,
            s.department_key,
            TO_NUMBER(TO_CHAR(TRY_TO_DATE(a.attendance_date, 'YYYY-MM-DD'), 'YYYYMMDD')) AS date_key,
            a.status,
            (a.status IN ('PRESENT','LATE'))  AS is_present,
            (a.status = 'EXCUSED')             AS is_excused,
            a.minutes_late::INTEGER            AS minutes_late,
            a.source_system
        FROM STAGING.STG_ATTENDANCE a
        JOIN PROD.DIM_STUDENT       s ON a.student_id  = s.student_id AND s.is_current = TRUE
        JOIN PROD.DIM_COURSE        c ON a.course_id   = c.course_id
    ) src
    ON  tgt.student_key = src.student_key
    AND tgt.course_key  = src.course_key
    AND tgt.date_key    = src.date_key
    WHEN NOT MATCHED THEN INSERT (
        student_key, course_key, instructor_key, department_key, date_key,
        status, is_present, is_excused, minutes_late, source_system
    ) VALUES (
        src.student_key, src.course_key, src.instructor_key, src.department_key, src.date_key,
        src.status, src.is_present, src.is_excused, src.minutes_late, src.source_system
    );
    """
    execute_statement(sql)
    count = execute_query("SELECT COUNT(*) AS cnt FROM PROD.FACT_ATTENDANCE")[0]["CNT"]
    logger.info(f"FACT_ATTENDANCE total records: {count:,}")


def refresh_semester_gpa() -> None:
    """Recompute FACT_SEMESTER_GPA aggregate from FACT_ACADEMIC_PERFORMANCE."""
    logger.info("Refreshing FACT_SEMESTER_GPA …")
    sql = """
    INSERT OVERWRITE INTO PROD.FACT_SEMESTER_GPA (
        student_key, department_key, academic_year, semester,
        semester_gpa, credit_hours_earned, credit_hours_attempted,
        courses_taken, courses_passed, avg_attendance_pct
    )
    SELECT
        student_key,
        department_key,
        academic_year,
        semester,
        ROUND(SUM(quality_points) / NULLIF(SUM(CASE WHEN grade_points IS NOT NULL
                                                    THEN credit_hours ELSE 0 END), 0), 3) AS semester_gpa,
        SUM(CASE WHEN is_passing THEN credit_hours ELSE 0 END)  AS credit_hours_earned,
        SUM(CASE WHEN grade_points IS NOT NULL THEN credit_hours ELSE 0 END) AS credit_hours_attempted,
        COUNT(*)                                                  AS courses_taken,
        SUM(is_passing::INTEGER)                                  AS courses_passed,
        ROUND(AVG(attendance_pct), 2)                            AS avg_attendance_pct
    FROM PROD.FACT_ACADEMIC_PERFORMANCE
    WHERE is_withdrawn  = FALSE
      AND is_incomplete = FALSE
    GROUP BY student_key, department_key, academic_year, semester;
    """
    execute_statement(sql)
    logger.info("FACT_SEMESTER_GPA refreshed")
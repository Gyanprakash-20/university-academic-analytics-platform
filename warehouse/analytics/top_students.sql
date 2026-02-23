-- =============================================================================
-- ANALYTICS: TOP STUDENTS  —  Window Functions + CTEs
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── 1. Top-N students by department for a given semester ──────────────────────
WITH semester_performance AS (
    SELECT
        s.student_id,
        s.first_name || ' ' || s.last_name        AS student_name,
        s.program,
        s.major,
        d.department_name,
        d.college,
        sg.academic_year,
        sg.semester,
        sg.semester_gpa,
        sg.cumulative_gpa,
        sg.credit_hours_earned,
        sg.avg_attendance_pct,
        -- Ranking within department
        RANK()         OVER (
            PARTITION BY d.department_name, sg.academic_year, sg.semester
            ORDER BY sg.semester_gpa DESC
        )                                          AS dept_rank,
        DENSE_RANK()   OVER (
            PARTITION BY sg.academic_year, sg.semester
            ORDER BY sg.semester_gpa DESC
        )                                          AS overall_rank,
        -- Percentile
        PERCENT_RANK() OVER (
            PARTITION BY d.department_name, sg.academic_year, sg.semester
            ORDER BY sg.semester_gpa
        )                                          AS dept_percentile,
        -- Trend: GPA change vs previous semester
        LAG(sg.semester_gpa) OVER (
            PARTITION BY s.student_id
            ORDER BY sg.academic_year, sg.semester
        )                                          AS prev_semester_gpa
    FROM
        FACT_SEMESTER_GPA       sg
        JOIN DIM_STUDENT        s  ON sg.student_key    = s.student_key AND s.is_current = TRUE
        JOIN DIM_DEPARTMENT     d  ON sg.department_key = d.department_key
),
top_students AS (
    SELECT
        *,
        semester_gpa - COALESCE(prev_semester_gpa, semester_gpa) AS gpa_delta,
        CASE
            WHEN dept_rank = 1                      THEN 'Top of Department'
            WHEN dept_percentile >= 0.90            THEN 'Top 10%'
            WHEN dept_percentile >= 0.75            THEN 'Top 25%'
            WHEN dept_percentile >= 0.50            THEN 'Above Average'
            ELSE                                         'Below Average'
        END                                               AS performance_tier
    FROM semester_performance
)
SELECT *
FROM   top_students
WHERE  dept_rank <= :top_n                      -- parameterized: e.g. 10
  AND  academic_year = :academic_year           -- e.g. '2024-2025'
  AND  semester      = :semester                -- e.g. 'Fall 2024'
ORDER  BY department_name, dept_rank;


-- ── 2. Dean's List students (semester GPA ≥ 3.5, full-time, ≥12 credits) ──────
WITH deans_list_candidates AS (
    SELECT
        s.student_id,
        s.first_name || ' ' || s.last_name  AS student_name,
        d.department_name,
        sg.academic_year,
        sg.semester,
        sg.semester_gpa,
        sg.credit_hours_earned,
        -- Running count of Dean's List semesters
        COUNT(*) OVER (
            PARTITION BY s.student_id
            ORDER BY sg.academic_year, sg.semester
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                    AS consecutive_dl_count,
        ROW_NUMBER() OVER (
            PARTITION BY s.student_id
            ORDER BY sg.academic_year DESC, sg.semester DESC
        )                                    AS recency_rank
    FROM
        FACT_SEMESTER_GPA    sg
        JOIN DIM_STUDENT     s  ON sg.student_key    = s.student_key AND s.is_current = TRUE
        JOIN DIM_DEPARTMENT  d  ON sg.department_key = d.department_key
    WHERE
        sg.semester_gpa        >= 3.5
        AND s.student_type     = 'Full-Time'
        AND sg.credit_hours_earned >= 12
)
SELECT
    student_id,
    student_name,
    department_name,
    academic_year,
    semester,
    ROUND(semester_gpa, 2)       AS semester_gpa,
    credit_hours_earned,
    consecutive_dl_count,
    CASE
        WHEN consecutive_dl_count >= 4 THEN '🏆 Presidential Scholar'
        WHEN consecutive_dl_count >= 2 THEN '⭐ Returning Dean''s List'
        ELSE                                '✅ Dean''s List'
    END                          AS honor_tier
FROM  deans_list_candidates
WHERE recency_rank = 1
ORDER BY semester_gpa DESC;


-- ── 3. At-Risk early warning  (low GPA + high absences) ───────────────────────
WITH student_risk_profile AS (
    SELECT
        s.student_id,
        s.first_name || ' ' || s.last_name      AS student_name,
        s.program,
        d.department_name,
        sg.academic_year,
        sg.semester,
        sg.semester_gpa,
        sg.cumulative_gpa,
        sg.avg_attendance_pct,
        sg.credit_hours_attempted,
        sg.credit_hours_earned,
        -- Attendance trend (moving average over last 3 semesters)
        AVG(sg.avg_attendance_pct) OVER (
            PARTITION BY s.student_id
            ORDER BY sg.academic_year, sg.semester
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                                        AS attendance_3sem_avg,
        -- GPA trend
        AVG(sg.semester_gpa) OVER (
            PARTITION BY s.student_id
            ORDER BY sg.academic_year, sg.semester
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                                        AS gpa_3sem_avg,
        LAG(sg.semester_gpa, 1) OVER (
            PARTITION BY s.student_id
            ORDER BY sg.academic_year, sg.semester
        )                                        AS gpa_last_sem
    FROM
        FACT_SEMESTER_GPA   sg
        JOIN DIM_STUDENT    s  ON sg.student_key    = s.student_key AND s.is_current = TRUE
        JOIN DIM_DEPARTMENT d  ON sg.department_key = d.department_key
),
risk_scores AS (
    SELECT
        *,
        -- Composite risk score (0–100, higher = more at risk)
        LEAST(100, GREATEST(0,
            (CASE WHEN semester_gpa      < 2.0  THEN 40 ELSE 0 END) +
            (CASE WHEN semester_gpa      < 2.5  THEN 20 ELSE 0 END) +
            (CASE WHEN avg_attendance_pct < 60  THEN 30 ELSE 0 END) +
            (CASE WHEN avg_attendance_pct < 75  THEN 15 ELSE 0 END) +
            (CASE WHEN gpa_3sem_avg IS NOT NULL
                       AND semester_gpa < gpa_3sem_avg - 0.5 THEN 15 ELSE 0 END) +
            (CASE WHEN credit_hours_earned < credit_hours_attempted * 0.75 THEN 10 ELSE 0 END)
        ))                                       AS risk_score
    FROM student_risk_profile
)
SELECT
    student_id,
    student_name,
    department_name,
    program,
    academic_year,
    semester,
    ROUND(semester_gpa,    2) AS semester_gpa,
    ROUND(cumulative_gpa,  2) AS cumulative_gpa,
    ROUND(avg_attendance_pct, 1) AS attendance_pct,
    risk_score,
    CASE
        WHEN risk_score >= 70 THEN 'CRITICAL'
        WHEN risk_score >= 50 THEN 'HIGH'
        WHEN risk_score >= 30 THEN 'MEDIUM'
        ELSE                       'LOW'
    END                       AS risk_level
FROM  risk_scores
WHERE risk_score >= 30
ORDER BY risk_score DESC;
-- =============================================================================
-- ANALYTICS: DEPARTMENT RANKING  —  CTEs + Window Functions + Partitioning
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── 1. Comprehensive department performance scorecard ─────────────────────────
WITH dept_semester_metrics AS (
    SELECT
        d.department_key,
        d.department_name,
        d.college,
        sg.academic_year,
        sg.semester,
        COUNT(DISTINCT sg.student_key)                          AS enrolled_students,
        ROUND(AVG(sg.semester_gpa), 3)                         AS avg_gpa,
        ROUND(MEDIAN(sg.semester_gpa), 3)                      AS median_gpa,
        ROUND(STDDEV(sg.semester_gpa), 3)                      AS gpa_stddev,
        ROUND(AVG(sg.avg_attendance_pct), 2)                   AS avg_attendance,
        SUM(sg.credit_hours_earned)                            AS total_credits_earned,
        SUM(sg.credit_hours_attempted)                         AS total_credits_attempted,
        SUM(sg.courses_passed)                                 AS courses_passed,
        SUM(sg.courses_taken)                                  AS courses_taken,
        COUNT(CASE WHEN sg.semester_gpa >= 3.5 THEN 1 END)    AS honors_students,
        COUNT(CASE WHEN sg.semester_gpa <  2.0 THEN 1 END)    AS probation_students
    FROM
        FACT_SEMESTER_GPA   sg
        JOIN DIM_DEPARTMENT d ON sg.department_key = d.department_key
    GROUP BY 1, 2, 3, 4, 5
),
dept_with_rates AS (
    SELECT
        *,
        ROUND(courses_passed * 1.0 / NULLIF(courses_taken, 0) * 100, 2)    AS pass_rate,
        ROUND(total_credits_earned * 1.0 /
              NULLIF(total_credits_attempted, 0) * 100, 2)                   AS credit_completion_rate,
        ROUND(honors_students * 1.0 / NULLIF(enrolled_students, 0) * 100, 2) AS honors_rate,
        ROUND(probation_students * 1.0 / NULLIF(enrolled_students, 0) * 100, 2) AS probation_rate,
        -- Compare to previous semester
        LAG(avg_gpa) OVER (
            PARTITION BY department_key
            ORDER BY academic_year, semester
        )                                                                    AS prev_avg_gpa,
        LAG(avg_attendance) OVER (
            PARTITION BY department_key
            ORDER BY academic_year, semester
        )                                                                    AS prev_attendance,
        LAG(enrolled_students) OVER (
            PARTITION BY department_key
            ORDER BY academic_year, semester
        )                                                                    AS prev_enrolled
    FROM dept_semester_metrics
),
dept_ranked AS (
    SELECT
        *,
        -- Overall GPA rank across all departments this semester
        RANK()          OVER (PARTITION BY academic_year, semester ORDER BY avg_gpa       DESC) AS gpa_rank,
        RANK()          OVER (PARTITION BY academic_year, semester ORDER BY avg_attendance DESC) AS attendance_rank,
        RANK()          OVER (PARTITION BY academic_year, semester ORDER BY pass_rate      DESC) AS pass_rate_rank,
        RANK()          OVER (PARTITION BY academic_year, semester ORDER BY honors_rate    DESC) AS honors_rank,
        -- Rank within college
        RANK()          OVER (
            PARTITION BY college, academic_year, semester
            ORDER BY avg_gpa DESC
        )                                                                                  AS college_gpa_rank,
        -- YoY GPA change
        ROUND(avg_gpa - COALESCE(prev_avg_gpa, avg_gpa), 3)                               AS gpa_delta,
        -- Composite performance score (weighted)
        ROUND(
            avg_gpa             * 0.40 +
            avg_attendance      * 0.02 +  -- scale from 100
            pass_rate           * 0.01 +
            (100 - probation_rate) * 0.01,
            2
        )                                                                                  AS performance_score
    FROM dept_with_rates
)
SELECT
    gpa_rank           AS overall_rank,
    department_name,
    college,
    academic_year,
    semester,
    enrolled_students,
    avg_gpa,
    median_gpa,
    gpa_stddev,
    ROUND(avg_attendance, 1) AS avg_attendance_pct,
    pass_rate,
    credit_completion_rate,
    honors_rate,
    probation_rate,
    gpa_delta,
    CASE
        WHEN gpa_delta > 0.1  THEN '📈 Improving'
        WHEN gpa_delta < -0.1 THEN '📉 Declining'
        ELSE                       '➡️ Stable'
    END                    AS trend,
    performance_score,
    college_gpa_rank
FROM dept_ranked
ORDER BY academic_year DESC, semester DESC, overall_rank;


-- ── 2. Course-level difficulty analysis ───────────────────────────────────────
WITH course_stats AS (
    SELECT
        c.course_code,
        c.course_name,
        c.credit_hours,
        c.level,
        c.category,
        d.department_name,
        f.academic_year,
        f.semester,
        COUNT(*)                                              AS enrollment_count,
        ROUND(AVG(f.grade_points), 3)                        AS avg_grade_points,
        ROUND(AVG(f.overall_score), 2)                       AS avg_score,
        ROUND(AVG(f.attendance_pct), 1)                      AS avg_attendance,
        COUNT(CASE WHEN f.grade_letter = 'A'  THEN 1 END)   AS a_count,
        COUNT(CASE WHEN f.grade_letter = 'B'  THEN 1 END)   AS b_count,
        COUNT(CASE WHEN f.grade_letter = 'C'  THEN 1 END)   AS c_count,
        COUNT(CASE WHEN f.grade_letter = 'D'  THEN 1 END)   AS d_count,
        COUNT(CASE WHEN f.grade_letter = 'F'  THEN 1 END)   AS f_count,
        COUNT(CASE WHEN f.is_withdrawn = TRUE THEN 1 END)   AS w_count,
        ROUND(STDDEV(f.grade_points), 3)                     AS grade_stddev
    FROM
        FACT_ACADEMIC_PERFORMANCE f
        JOIN DIM_COURSE           c ON f.course_key     = c.course_key
        JOIN DIM_DEPARTMENT       d ON f.department_key = d.department_key
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT
    course_code,
    course_name,
    department_name,
    level,
    academic_year,
    semester,
    enrollment_count,
    avg_grade_points,
    avg_score,
    avg_attendance,
    a_count, b_count, c_count, d_count, f_count, w_count,
    ROUND(f_count * 1.0 / NULLIF(enrollment_count, 0) * 100, 1) AS fail_rate,
    ROUND(w_count * 1.0 / NULLIF(enrollment_count, 0) * 100, 1) AS withdrawal_rate,
    -- Difficulty rating (inverse of avg grade)
    RANK() OVER (
        PARTITION BY department_name, academic_year, semester
        ORDER BY avg_grade_points ASC
    )                                                             AS difficulty_rank,
    CASE
        WHEN avg_grade_points >= 3.5 THEN 'Easy'
        WHEN avg_grade_points >= 2.5 THEN 'Moderate'
        WHEN avg_grade_points >= 2.0 THEN 'Challenging'
        ELSE                               'Very Hard'
    END                                                           AS difficulty_tier
FROM course_stats
ORDER BY department_name, fail_rate DESC;
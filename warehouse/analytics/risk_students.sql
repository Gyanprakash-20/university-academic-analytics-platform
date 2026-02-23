-- =============================================================================
-- ANALYTICS: AT-RISK STUDENT IDENTIFICATION
-- Early warning system using GPA, attendance, and trend analysis
-- =============================================================================
USE DATABASE UNIVERSITY_DW;
USE SCHEMA PROD;

-- ── 1. Comprehensive At-Risk Scoring Model ────────────────────────────────────
-- Grain: one row per student per semester with composite risk score
CREATE OR REPLACE VIEW V_AT_RISK_STUDENTS AS
WITH student_history AS (
    SELECT
        sg.student_key,
        sg.department_key,
        sg.academic_year,
        sg.semester,
        sg.semester_gpa,
        sg.cumulative_gpa,
        sg.credit_hours_earned,
        sg.credit_hours_attempted,
        sg.courses_taken,
        sg.courses_passed,
        sg.avg_attendance_pct,
        -- Prior semester values
        LAG(sg.semester_gpa, 1) OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
        ) AS prev_gpa_1sem,
        LAG(sg.semester_gpa, 2) OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
        ) AS prev_gpa_2sem,
        LAG(sg.avg_attendance_pct, 1) OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
        ) AS prev_attendance_1sem,
        -- Running counts of risk indicators
        COUNT(CASE WHEN sg.semester_gpa < 2.0 THEN 1 END) OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS low_gpa_semester_count,
        COUNT(CASE WHEN sg.avg_attendance_pct < 75 THEN 1 END) OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS low_attendance_semester_count,
        -- 3-semester rolling average GPA
        AVG(sg.semester_gpa) OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3sem_gpa,
        -- 3-semester rolling attendance
        AVG(sg.avg_attendance_pct) OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3sem_attendance,
        -- Semester sequence number (1 = first semester)
        ROW_NUMBER() OVER (
            PARTITION BY sg.student_key
            ORDER BY sg.academic_year, sg.semester
        ) AS semester_sequence
    FROM FACT_SEMESTER_GPA sg
),
risk_components AS (
    SELECT
        sh.*,
        s.student_id,
        s.first_name || ' ' || s.last_name AS student_name,
        s.program,
        s.major,
        s.student_type,
        s.enrollment_date,
        d.department_name,
        d.college,

        -- ── Risk component scores (each 0–20 points) ─────────────────────
        -- Component 1: Current GPA (max 30 pts)
        CASE
            WHEN sh.semester_gpa < 1.0 THEN 30
            WHEN sh.semester_gpa < 1.5 THEN 25
            WHEN sh.semester_gpa < 2.0 THEN 20
            WHEN sh.semester_gpa < 2.5 THEN 12
            WHEN sh.semester_gpa < 3.0 THEN 5
            ELSE 0
        END AS gpa_risk_score,

        -- Component 2: Attendance (max 25 pts)
        CASE
            WHEN sh.avg_attendance_pct < 50  THEN 25
            WHEN sh.avg_attendance_pct < 60  THEN 20
            WHEN sh.avg_attendance_pct < 70  THEN 15
            WHEN sh.avg_attendance_pct < 75  THEN 10
            WHEN sh.avg_attendance_pct < 85  THEN 3
            ELSE 0
        END AS attendance_risk_score,

        -- Component 3: GPA trend (declining = more risk, max 20 pts)
        CASE
            WHEN sh.prev_gpa_1sem IS NULL THEN 0
            WHEN (sh.semester_gpa - sh.prev_gpa_1sem) < -1.0 THEN 20
            WHEN (sh.semester_gpa - sh.prev_gpa_1sem) < -0.5 THEN 12
            WHEN (sh.semester_gpa - sh.prev_gpa_1sem) < -0.25 THEN 6
            ELSE 0
        END AS gpa_trend_risk_score,

        -- Component 4: Credit completion (max 15 pts)
        CASE
            WHEN sh.credit_hours_attempted = 0 THEN 0
            WHEN (sh.credit_hours_earned * 1.0 / sh.credit_hours_attempted) < 0.5 THEN 15
            WHEN (sh.credit_hours_earned * 1.0 / sh.credit_hours_attempted) < 0.7 THEN 8
            WHEN (sh.credit_hours_earned * 1.0 / sh.credit_hours_attempted) < 0.85 THEN 3
            ELSE 0
        END AS credit_completion_risk_score,

        -- Component 5: Repeat offender (multiple bad semesters, max 10 pts)
        CASE
            WHEN sh.low_gpa_semester_count >= 3 THEN 10
            WHEN sh.low_gpa_semester_count >= 2 THEN 6
            WHEN sh.low_gpa_semester_count >= 1 THEN 2
            ELSE 0
        END AS repeat_risk_score
    FROM student_history sh
    JOIN DIM_STUDENT     s  ON sh.student_key    = s.student_key AND s.is_current = TRUE
    JOIN DIM_DEPARTMENT  d  ON sh.department_key = d.department_key
),
final_risk AS (
    SELECT
        *,
        -- Composite risk score (0–100)
        LEAST(100, gpa_risk_score + attendance_risk_score + gpa_trend_risk_score
                   + credit_completion_risk_score + repeat_risk_score) AS total_risk_score,

        -- Risk tier
        CASE
            WHEN LEAST(100, gpa_risk_score + attendance_risk_score + gpa_trend_risk_score
                            + credit_completion_risk_score + repeat_risk_score) >= 65 THEN 'CRITICAL'
            WHEN LEAST(100, gpa_risk_score + attendance_risk_score + gpa_trend_risk_score
                            + credit_completion_risk_score + repeat_risk_score) >= 45 THEN 'HIGH'
            WHEN LEAST(100, gpa_risk_score + attendance_risk_score + gpa_trend_risk_score
                            + credit_completion_risk_score + repeat_risk_score) >= 25 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_tier,

        -- Primary risk reason
        CASE
            WHEN gpa_risk_score >= 20           THEN 'Low GPA'
            WHEN attendance_risk_score >= 15    THEN 'Poor Attendance'
            WHEN gpa_trend_risk_score >= 12     THEN 'Declining GPA Trend'
            WHEN credit_completion_risk_score >= 8 THEN 'Low Credit Completion'
            WHEN repeat_risk_score >= 6         THEN 'Repeated Academic Issues'
            ELSE 'Combined Risk Factors'
        END AS primary_risk_reason,

        -- Recommended intervention
        CASE
            WHEN gpa_risk_score >= 20 AND attendance_risk_score >= 15
                THEN 'Urgent: Academic + Attendance Intervention Required'
            WHEN gpa_risk_score >= 20
                THEN 'Schedule Academic Counseling & Tutoring Support'
            WHEN attendance_risk_score >= 15
                THEN 'Contact Student – Attendance Policy Warning'
            WHEN gpa_trend_risk_score >= 12
                THEN 'Monitor Closely – Schedule Check-in with Advisor'
            WHEN repeat_risk_score >= 6
                THEN 'Academic Probation Review – Dean Notification'
            ELSE 'Advisor Outreach Recommended'
        END AS recommended_action
    FROM risk_components
)
SELECT
    student_id,
    student_name,
    program,
    major,
    student_type,
    department_name,
    college,
    academic_year,
    semester,
    ROUND(semester_gpa, 2)          AS semester_gpa,
    ROUND(cumulative_gpa, 2)        AS cumulative_gpa,
    ROUND(avg_attendance_pct, 1)    AS attendance_pct,
    credit_hours_earned,
    credit_hours_attempted,
    ROUND(credit_hours_earned * 1.0 / NULLIF(credit_hours_attempted, 0) * 100, 1) AS credit_completion_pct,
    courses_passed,
    courses_taken,
    ROUND(prev_gpa_1sem, 2)         AS prev_semester_gpa,
    ROUND(rolling_3sem_gpa, 2)      AS rolling_3sem_avg_gpa,
    low_gpa_semester_count,
    low_attendance_semester_count,
    semester_sequence,
    -- Risk scores
    gpa_risk_score,
    attendance_risk_score,
    gpa_trend_risk_score,
    credit_completion_risk_score,
    repeat_risk_score,
    total_risk_score,
    risk_tier,
    primary_risk_reason,
    recommended_action
FROM final_risk
WHERE total_risk_score >= 25;   -- Medium risk and above only


-- ── 2. Academic Probation Candidates ─────────────────────────────────────────
-- Students who meet probation criteria (GPA < 2.0 for current semester)
CREATE OR REPLACE VIEW V_PROBATION_CANDIDATES AS
SELECT
    s.student_id,
    s.first_name || ' ' || s.last_name AS student_name,
    s.program,
    d.department_name,
    sg.academic_year,
    sg.semester,
    ROUND(sg.semester_gpa, 2)    AS semester_gpa,
    ROUND(sg.cumulative_gpa, 2)  AS cumulative_gpa,
    sg.credit_hours_attempted,
    sg.credit_hours_earned,
    ROUND(sg.avg_attendance_pct, 1) AS attendance_pct,
    -- Count of prior probation-level semesters
    COUNT(*) OVER (
        PARTITION BY sg.student_key
        ORDER BY sg.academic_year, sg.semester
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prior_low_gpa_semesters,
    CASE
        WHEN sg.cumulative_gpa < 2.0 THEN 'Academic Suspension Candidate'
        WHEN sg.semester_gpa   < 2.0 THEN 'Academic Probation'
        ELSE 'Warning'
    END AS probation_status
FROM FACT_SEMESTER_GPA    sg
JOIN DIM_STUDENT          s  ON sg.student_key    = s.student_key AND s.is_current = TRUE
JOIN DIM_DEPARTMENT       d  ON sg.department_key = d.department_key
WHERE sg.semester_gpa < 2.0
ORDER BY sg.semester_gpa ASC;


-- ── 3. First-Generation & High-Need Student Monitoring ───────────────────────
-- Tracks scholarship students with declining performance
CREATE OR REPLACE VIEW V_SCHOLARSHIP_AT_RISK AS
SELECT
    s.student_id,
    s.first_name || ' ' || s.last_name AS student_name,
    s.program,
    d.department_name,
    s.scholarship_amount,
    sg.academic_year,
    sg.semester,
    ROUND(sg.semester_gpa, 2) AS semester_gpa,
    -- Scholarship minimum GPA is typically 3.0
    CASE WHEN sg.semester_gpa < 3.0 THEN TRUE ELSE FALSE END AS below_scholarship_threshold,
    CASE WHEN sg.semester_gpa < 3.0
         THEN 'Scholarship at risk — GPA below 3.0 minimum'
         ELSE 'Scholarship maintained'
    END AS scholarship_status
FROM FACT_SEMESTER_GPA sg
JOIN DIM_STUDENT       s  ON sg.student_key    = s.student_key
                          AND s.is_current     = TRUE
                          AND s.scholarship    = TRUE
JOIN DIM_DEPARTMENT    d  ON sg.department_key = d.department_key
ORDER BY sg.semester_gpa ASC;


-- ── 4. Dropout Prediction Signal ─────────────────────────────────────────────
-- Students who stopped enrolling (gap in semester sequence)
WITH enrollment_gaps AS (
    SELECT
        student_key,
        academic_year,
        semester,
        semester_gpa,
        LAG(academic_year) OVER (
            PARTITION BY student_key ORDER BY academic_year, semester
        ) AS prev_year,
        LAG(semester) OVER (
            PARTITION BY student_key ORDER BY academic_year, semester
        ) AS prev_semester,
        ROW_NUMBER() OVER (
            PARTITION BY student_key ORDER BY academic_year DESC, semester DESC
        ) AS recency_rank
    FROM FACT_SEMESTER_GPA
)
SELECT
    s.student_id,
    s.first_name || ' ' || s.last_name AS student_name,
    d.department_name,
    eg.academic_year         AS last_enrolled_year,
    eg.semester              AS last_enrolled_semester,
    ROUND(eg.semester_gpa, 2) AS last_semester_gpa,
    'Potential Dropout – No Recent Enrollment' AS flag
FROM enrollment_gaps eg
JOIN DIM_STUDENT     s  ON eg.student_key    = s.student_key AND s.is_current = TRUE
JOIN DIM_DEPARTMENT  d  ON s.department_key  = d.department_key
WHERE eg.recency_rank = 1
  AND eg.academic_year < '2024-2025'   -- Not enrolled in current year
ORDER BY eg.academic_year, eg.semester;
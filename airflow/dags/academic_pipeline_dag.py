"""
Airflow DAG: University Academic Analytics Pipeline
====================================================
Schedule: Daily at 02:00 UTC
Flow: Extract → Validate → Stage → Merge → Spark Batch → Notifications
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ───────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["data-eng@university.edu"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ── Task callables ─────────────────────────────────────────────────────────────
def extract_sis_task(**ctx):
    from etl.extract.extract_sis import extract_sis
    paths = extract_sis()
    ctx["ti"].xcom_push(key="sis_paths", value={k: str(v) for k, v in paths.items()})
    return paths


def extract_lms_task(**ctx):
    from etl.extract.extract_lms import extract_lms
    paths = extract_lms()
    ctx["ti"].xcom_push(key="lms_paths", value={k: str(v) for k, v in paths.items()})
    return paths


def extract_attendance_task(**ctx):
    import pandas as pd
    from pathlib import Path
    from etl.extract.extract_attendance import extract_attendance

    students_df = pd.read_csv("data/raw/students.csv")
    courses_df  = pd.read_csv("data/raw/courses.csv")
    paths = extract_attendance(
        student_ids=students_df["student_id"].tolist(),
        course_ids=courses_df["course_id"].tolist(),
    )
    ctx["ti"].xcom_push(key="att_paths", value={k: str(v) for k, v in paths.items()})


def validate_transform_task(**ctx):
    import pandas as pd
    from pathlib import Path
    from etl.transform.clean_students import clean_students
    from etl.transform.validation_rules import validate_students, validate_enrollments

    students_df = pd.read_csv("data/raw/students.csv")
    clean_stud, rejected = clean_students(students_df)
    report = validate_students(clean_stud)
    if not report.passed:
        raise ValueError(f"Validation failed: {report.summary()}")

    ctx["ti"].xcom_push(key="val_summary", value=report.summary())
    # Cache clean data
    clean_stud.to_csv("data/staging/clean_students.csv", index=False)
    rejected.to_csv("data/staging/rejected_students.csv",  index=False)


def stage_data_task(**ctx):
    import pandas as pd
    from etl.load.load_staging import (
        stage_students, stage_courses, stage_instructors,
        stage_enrollments, stage_attendance,
    )

    counts = {
        "students":    stage_students(pd.read_csv("data/staging/clean_students.csv")),
        "courses":     stage_courses(pd.read_csv("data/raw/courses.csv")),
        "instructors": stage_instructors(pd.read_csv("data/raw/instructors.csv")),
        "enrollments": stage_enrollments(pd.read_csv("data/raw/enrollments.csv")),
        "attendance":  stage_attendance(pd.read_csv("data/raw/attendance.csv")),
    }
    ctx["ti"].xcom_push(key="stage_counts", value=counts)


def merge_warehouse_task(**ctx):
    from etl.load.merge_fact_tables import (
        merge_dim_student, merge_fact_performance,
        merge_fact_attendance, refresh_semester_gpa,
    )
    merge_dim_student()
    merge_fact_performance()
    merge_fact_attendance()
    refresh_semester_gpa()


def submit_spark_batch(**ctx):
    import subprocess
    result = subprocess.run(
        ["spark-submit", "spark_jobs/batch/semester_aggregation.py"],
        capture_output=True, text=True, check=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Spark job failed: {result.stderr}")


def check_data_quality(**ctx):
    """Branch: proceed or alert on quality issues."""
    val_summary = ctx["ti"].xcom_pull(task_ids="validate_transform", key="val_summary")
    if val_summary and float(val_summary.get("pass_rate", "0%").rstrip("%")) >= 95:
        return "merge_warehouse"
    return "quality_alert"


def send_quality_alert(**ctx):
    from config.logging_config import logger
    logger.error("Data quality below threshold — pipeline halted")
    # In production: send email / Slack notification


# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id         = "university_academic_pipeline",
    default_args   = DEFAULT_ARGS,
    description    = "Daily ETL pipeline for university academic analytics",
    schedule       = "0 2 * * *",          # 02:00 UTC daily
    start_date     = datetime(2024, 1, 1),
    catchup        = False,
    max_active_runs = 1,
    tags           = ["university", "etl", "academic"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # ── Extract ────────────────────────────────────────────────────────────────
    extract_sis = PythonOperator(
        task_id         = "extract_sis",
        python_callable = extract_sis_task,
    )
    extract_lms = PythonOperator(
        task_id         = "extract_lms",
        python_callable = extract_lms_task,
    )
    extract_att = PythonOperator(
        task_id         = "extract_attendance",
        python_callable = extract_attendance_task,
    )

    # ── Validate + Transform ───────────────────────────────────────────────────
    validate = PythonOperator(
        task_id         = "validate_transform",
        python_callable = validate_transform_task,
    )

    # ── Quality Gate ───────────────────────────────────────────────────────────
    quality_gate = BranchPythonOperator(
        task_id         = "quality_gate",
        python_callable = check_data_quality,
    )
    quality_alert = PythonOperator(
        task_id         = "quality_alert",
        python_callable = send_quality_alert,
    )

    # ── Stage ──────────────────────────────────────────────────────────────────
    stage = PythonOperator(
        task_id         = "stage_data",
        python_callable = stage_data_task,
    )

    # ── Merge ──────────────────────────────────────────────────────────────────
    merge = PythonOperator(
        task_id         = "merge_warehouse",
        python_callable = merge_warehouse_task,
    )

    # ── Spark Batch ────────────────────────────────────────────────────────────
    spark_batch = PythonOperator(
        task_id         = "spark_semester_aggregation",
        python_callable = submit_spark_batch,
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    start >> [extract_sis, extract_lms]
    [extract_sis, extract_lms] >> extract_att
    extract_att >> validate >> quality_gate
    quality_gate >> [stage, quality_alert]
    stage >> merge >> spark_batch >> end
    quality_alert >> end
"""
Main ETL Pipeline
==================
Orchestrates Extract → Validate → Transform → Load for all entities.
Can be run standalone or called from Airflow DAG tasks.
"""
from __future__ import annotations

import time
import uuid
from pathlib import Path

import pandas as pd

from config.logging_config import logger
from config.settings import settings

from etl.extract.extract_sis        import extract_sis
from etl.extract.extract_lms        import extract_lms
from etl.extract.extract_attendance import extract_attendance

from etl.transform.clean_students   import clean_students
from etl.transform.calculate_gpa    import full_gpa_pipeline, enrich_grades
from etl.transform.validation_rules import (
    validate_students, validate_enrollments, validate_attendance
)

from etl.load.load_staging          import (
    stage_students, stage_courses, stage_instructors,
    stage_enrollments, stage_attendance, load_rejected_records,
)


RAW_DIR = Path("data/raw")
RUN_ID  = str(uuid.uuid4())


def run_extract() -> dict[str, Path]:
    """Phase 1: Extract all source systems."""
    logger.info("=" * 60)
    logger.info("ETL PHASE 1: EXTRACT")
    logger.info("=" * 60)
    t = time.time()

    sis_paths = extract_sis(output_dir=RAW_DIR)
    lms_paths = extract_lms(output_dir=RAW_DIR)

    students_df = pd.read_csv(sis_paths["students"])
    courses_df  = pd.read_csv(lms_paths["courses"])

    att_paths = extract_attendance(
        student_ids=students_df["student_id"].tolist(),
        course_ids=courses_df["course_id"].tolist(),
        output_dir=RAW_DIR,
    )

    all_paths = {**sis_paths, **lms_paths, **att_paths}
    logger.info(f"Extract complete in {time.time()-t:.1f}s: {list(all_paths.keys())}")
    return all_paths


def run_validate_transform(paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    """Phase 2: Validate + Transform."""
    logger.info("=" * 60)
    logger.info("ETL PHASE 2: VALIDATE + TRANSFORM")
    logger.info("=" * 60)
    t = time.time()

    # -- Students
    raw_students = pd.read_csv(paths["students"])
    clean_stud, rejected_stud = clean_students(raw_students)
    val_report = validate_students(clean_stud)
    logger.info(f"Students validation: {val_report.summary()}")
    if not val_report.passed:
        logger.error("Student validation FAILED — aborting pipeline")
        raise RuntimeError("Student validation failed")

    # -- Courses & Instructors (minimal transform needed)
    courses_df     = pd.read_csv(paths["courses"])
    instructors_df = pd.read_csv(paths["instructors"])

    # -- Enrollments
    raw_enrollments = pd.read_csv(paths["enrollments"])
    enrollments_df  = enrich_grades(raw_enrollments)
    val_enr = validate_enrollments(enrollments_df, clean_stud)
    logger.info(f"Enrollments validation: {val_enr.summary()}")

    # -- GPA Computation
    gpa_df = full_gpa_pipeline(raw_enrollments)
    logger.info(f"GPA computed for {gpa_df['student_id'].nunique():,} students")

    # -- Attendance
    raw_attendance = pd.read_csv(paths["attendance"])
    val_att = validate_attendance(raw_attendance)
    logger.info(f"Attendance validation: {val_att.summary()}")

    logger.info(f"Transform complete in {time.time()-t:.1f}s")
    return {
        "students":    clean_stud,
        "rejected":    rejected_stud,
        "courses":     courses_df,
        "instructors": instructors_df,
        "enrollments": enrollments_df,
        "gpa":         gpa_df,
        "attendance":  raw_attendance,
    }


def run_load(dfs: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Phase 3: Load into staging tables."""
    logger.info("=" * 60)
    logger.info("ETL PHASE 3: LOAD → STAGING")
    logger.info("=" * 60)
    t = time.time()

    counts = {
        "students":    stage_students(dfs["students"]),
        "courses":     stage_courses(dfs["courses"]),
        "instructors": stage_instructors(dfs["instructors"]),
        "enrollments": stage_enrollments(dfs["enrollments"]),
        "attendance":  stage_attendance(dfs["attendance"]),
    }

    if not dfs["rejected"].empty:
        load_rejected_records(dfs["rejected"], entity="students")

    logger.info(f"Load complete in {time.time()-t:.1f}s: {counts}")
    return counts


def run_warehouse_merge() -> None:
    """Phase 4: Merge staging → production (Snowflake only)."""
    if settings.env != "production":
        logger.info("Skipping Snowflake MERGE (non-production environment)")
        return

    logger.info("=" * 60)
    logger.info("ETL PHASE 4: MERGE → PRODUCTION WAREHOUSE")
    logger.info("=" * 60)
    from etl.load.merge_fact_tables import (
        merge_dim_student, merge_fact_performance,
        merge_fact_attendance, refresh_semester_gpa,
    )
    merge_dim_student()
    merge_fact_performance()
    merge_fact_attendance()
    refresh_semester_gpa()


def run_full_pipeline() -> None:
    """Execute the complete ETL pipeline end-to-end."""
    start = time.time()
    logger.info(f"🚀 Starting University Analytics ETL Pipeline (run_id={RUN_ID})")

    try:
        paths = run_extract()
        dfs   = run_validate_transform(paths)
        counts = run_load(dfs)
        run_warehouse_merge()

        elapsed = time.time() - start
        logger.info(f"✅ Pipeline completed in {elapsed:.1f}s | rows loaded: {counts}")

    except Exception as exc:
        logger.error(f"❌ Pipeline FAILED after {time.time()-start:.1f}s: {exc}")
        raise


if __name__ == "__main__":
    run_full_pipeline()
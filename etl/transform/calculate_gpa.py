"""
Transform: GPA Calculation Engine
===================================
Computes:
  - Course grade points
  - Semester GPA
  - Cumulative GPA
  - Quality points
  - Pass/fail flags
"""
from __future__ import annotations

import pandas as pd
import numpy as np

from config.logging_config import logger


# ── Grade → Points mapping ─────────────────────────────────────────────────────
GRADE_POINTS: dict[str, float] = {
    "A+": 4.0, "A": 4.0, "A-": 3.7,
    "B+": 3.3, "B": 3.0, "B-": 2.7,
    "C+": 2.3, "C": 2.0, "C-": 1.7,
    "D+": 1.3, "D": 1.0, "D-": 0.7,
    "F":  0.0,
    "W":  None,   # Withdrawal — excluded from GPA calculation
    "I":  None,   # Incomplete — excluded
    "AU": None,   # Audit
}

PASSING_GRADES = {"A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-"}
CREDIT_HOURS_MAP = {3: 3, 4: 4, 1: 1}   # Default all to 3 if not mapped


def enrich_grades(enrollments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add computed columns to enrollment DataFrame:
      - grade_points (numeric)
      - quality_points  (grade_points × credit_hours)
      - is_passing
      - is_withdrawn
      - is_incomplete
    """
    df = enrollments_df.copy()

    # Ensure grade column is uppercase/stripped
    df["grade"] = df["grade"].astype(str).str.strip().str.upper()

    # Map grade to grade_points
    df["grade_points"] = df["grade"].map(GRADE_POINTS)

    # Override with provided grade_points if valid numeric
    if "grade_points" in enrollments_df.columns:
        provided = pd.to_numeric(enrollments_df["grade_points"], errors="coerce")
        valid_provided = provided.notna()
        df.loc[valid_provided, "grade_points"] = provided[valid_provided]

    # Flags
    df["is_passing"]    = df["grade"].isin(PASSING_GRADES)
    df["is_withdrawn"]  = df["grade"] == "W"
    df["is_incomplete"] = df["grade"] == "I"

    # Credit hours: default 3 if not provided
    if "credit_hours" not in df.columns:
        df["credit_hours"] = 3

    df["credit_hours"] = pd.to_numeric(df["credit_hours"], errors="coerce").fillna(3).astype(int)

    # Quality points (only for gradable courses)
    df["quality_points"] = df["grade_points"] * df["credit_hours"]

    logger.debug(f"Enriched grades for {len(df):,} enrollments")
    return df


def calculate_semester_gpa(enrollments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute GPA per student × semester from enrollment records.

    Returns
    -------
    DataFrame with columns:
        student_id, academic_year, semester,
        semester_gpa, credit_hours_earned, credit_hours_attempted,
        courses_taken, courses_passed, avg_attendance_pct
    """
    df = enrich_grades(enrollments_df)

    # Only GPA-eligible rows (exclude W, I, AU)
    gpa_eligible = df[df["grade_points"].notna()].copy()

    # Group by student × semester
    agg = (
        gpa_eligible
        .groupby(["student_id", "academic_year", "semester"])
        .agg(
            total_quality_points = ("quality_points",   "sum"),
            total_credit_hours   = ("credit_hours",     "sum"),
            credit_hours_earned  = ("credit_hours",     lambda x: x[df.loc[x.index, "is_passing"]].sum()),
            courses_taken        = ("enrollment_id",    "count"),
            courses_passed       = ("is_passing",       "sum"),
            avg_attendance_pct   = ("attendance_pct",   "mean"),
            midterm_avg          = ("midterm_score",    "mean"),
            final_avg            = ("final_score",      "mean"),
        )
        .reset_index()
    )

    # Semester GPA = total quality points / total attempted credit hours
    agg["semester_gpa"] = (
        agg["total_quality_points"] / agg["total_credit_hours"].replace(0, np.nan)
    ).round(3)

    agg["semester_gpa"]        = agg["semester_gpa"].clip(0, 4.0)
    agg["credit_hours_attempted"] = agg["total_credit_hours"]
    agg["avg_attendance_pct"]  = agg["avg_attendance_pct"].round(2)

    # Drop helper columns
    agg = agg.drop(columns=["total_quality_points", "total_credit_hours"])

    logger.info(f"Computed semester GPA for {len(agg):,} student-semester combinations")
    return agg


def calculate_cumulative_gpa(semester_gpa_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add cumulative GPA column by computing running weighted average
    ordered by (academic_year, semester).
    """
    df = semester_gpa_df.copy()

    # Build a numeric sort key: academic_year(YYYY) + semester offset
    SEMESTER_ORDER = {"Fall": 1, "Spring": 2, "Summer": 3}

    def sem_sort_key(row) -> float:
        year  = int(row["academic_year"].split("-")[0])
        sem   = row["semester"].split()[0]
        order = SEMESTER_ORDER.get(sem, 0)
        return year * 10 + order

    df["_sort_key"] = df.apply(sem_sort_key, axis=1)
    df = df.sort_values(["student_id", "_sort_key"])

    # Cumulative GPA: running sum of quality points / running credit hours
    df["_qp_running"]    = df.groupby("student_id")["semester_gpa"].transform(
        lambda x: (x * df.loc[x.index, "credit_hours_attempted"]).cumsum()
    )
    df["_ch_running"]    = df.groupby("student_id")["credit_hours_attempted"].cumsum()
    df["cumulative_gpa"] = (df["_qp_running"] / df["_ch_running"].replace(0, np.nan)).round(3)
    df["cumulative_gpa"] = df["cumulative_gpa"].clip(0, 4.0)

    df = df.drop(columns=["_sort_key", "_qp_running", "_ch_running"])
    logger.info(f"Computed cumulative GPA for {df['student_id'].nunique():,} students")
    return df


def full_gpa_pipeline(enrollments_df: pd.DataFrame) -> pd.DataFrame:
    """
    End-to-end: raw enrollments → semester GPA + cumulative GPA.
    """
    sem_gpa = calculate_semester_gpa(enrollments_df)
    cum_gpa = calculate_cumulative_gpa(sem_gpa)
    return cum_gpa
"""
Transform: Clean & validate student data
=========================================
- Type coercion and date parsing
- Null / anomaly detection
- Deduplication
- SCD Type 2 change detection (hash comparison)
"""
from __future__ import annotations

import hashlib
from datetime import date

import pandas as pd
import numpy as np

from config.logging_config import logger


# ── Schema definition ──────────────────────────────────────────────────────────
EXPECTED_COLS = {
    "student_id", "first_name", "last_name", "email", "date_of_birth",
    "gender", "nationality", "enrollment_date", "program", "major",
    "department_id", "student_type", "scholarship", "scholarship_amount",
}

VALID_STUDENT_TYPES  = {"Full-Time", "Part-Time"}
VALID_GENDERS        = {"Male", "Female", "Non-Binary", "Prefer not to say"}
MIN_ENROLLMENT_YEAR  = 2000
MAX_ENROLLMENT_YEAR  = date.today().year + 1


def clean_students(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean raw student DataFrame.

    Returns
    -------
    clean_df   : validated, transformed records
    rejected_df: records that failed validation with error reasons
    """
    logger.info(f"Starting student clean: {len(df):,} rows")
    df = df.copy()

    # ── 1. Column validation ──────────────────────────────────────────────────
    missing_cols = EXPECTED_COLS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # ── 2. Trim whitespace ────────────────────────────────────────────────────
    str_cols = df.select_dtypes("object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # ── 3. Type coercions ─────────────────────────────────────────────────────
    df["date_of_birth"]   = pd.to_datetime(df["date_of_birth"],   errors="coerce").dt.date
    df["enrollment_date"] = pd.to_datetime(df["enrollment_date"], errors="coerce").dt.date
    df["scholarship"]     = df["scholarship"].map(
        lambda x: True if str(x).lower() in ("true", "1", "yes") else False
    )
    df["scholarship_amount"] = pd.to_numeric(df["scholarship_amount"], errors="coerce").fillna(0.0)

    # ── 4. Validation rules ───────────────────────────────────────────────────
    errors: dict[int, list[str]] = {i: [] for i in df.index}

    # student_id must be unique and non-null
    null_id = df["student_id"].isna()
    df.loc[null_id].apply(lambda r: errors[r.name].append("NULL student_id"), axis=1)

    dup_ids = df[df.duplicated("student_id", keep=False) & ~null_id]["student_id"].unique()
    if len(dup_ids):
        logger.warning(f"Found {len(dup_ids)} duplicate student_ids: {dup_ids[:5]}")
        df = df.drop_duplicates("student_id", keep="last")

    # Email format
    bad_email = ~df["email"].str.contains(r"^[\w\.\+\-]+@[\w\-]+\.\w{2,}$", na=False)
    for idx in df[bad_email].index:
        errors[idx].append(f"Invalid email: {df.at[idx, 'email']}")

    # Date of birth sanity
    today = date.today()
    for idx, row in df.iterrows():
        dob = row["date_of_birth"]
        if pd.isna(dob):
            errors[idx].append("NULL date_of_birth")
        else:
            age = (today - dob).days / 365.25
            if age < 15 or age > 80:
                errors[idx].append(f"Unrealistic age: {age:.1f} years")

    # Enrollment date
    for idx, row in df.iterrows():
        ed = row["enrollment_date"]
        if pd.isna(ed):
            errors[idx].append("NULL enrollment_date")
        elif ed.year < MIN_ENROLLMENT_YEAR or ed.year > MAX_ENROLLMENT_YEAR:
            errors[idx].append(f"Enrollment year out of range: {ed.year}")

    # Student type
    bad_type = ~df["student_type"].isin(VALID_STUDENT_TYPES)
    for idx in df[bad_type].index:
        errors[idx].append(f"Invalid student_type: {df.at[idx, 'student_type']}")

    # ── 5. Split clean / rejected ─────────────────────────────────────────────
    error_rows  = {idx: e for idx, e in errors.items() if idx in df.index and e}
    clean_mask  = ~df.index.isin(error_rows.keys())
    clean_df    = df[clean_mask].copy()
    rejected_df = df[~clean_mask].copy()
    rejected_df["validation_errors"] = rejected_df.index.map(
        lambda i: "; ".join(error_rows.get(i, []))
    )

    # ── 6. Add SCD hash (for Type 2 change detection) ─────────────────────────
    scd_cols = ["first_name", "last_name", "email", "program", "major",
                 "department_id", "student_type", "scholarship", "scholarship_amount"]
    clean_df["row_hash"] = clean_df[scd_cols].astype(str).apply(
        lambda row: hashlib.md5("|".join(row).encode()).hexdigest(), axis=1
    )

    # ── 7. Normalize text fields ───────────────────────────────────────────────
    clean_df["email"]        = clean_df["email"].str.lower()
    clean_df["first_name"]   = clean_df["first_name"].str.title()
    clean_df["last_name"]    = clean_df["last_name"].str.title()
    clean_df["nationality"]  = clean_df["nationality"].str.strip()
    clean_df["gender"]       = clean_df["gender"].fillna("Prefer not to say")

    logger.info(
        f"Student clean complete: {len(clean_df):,} clean, "
        f"{len(rejected_df):,} rejected "
        f"({len(rejected_df)/max(1,len(df))*100:.1f}% rejection rate)"
    )
    return clean_df, rejected_df
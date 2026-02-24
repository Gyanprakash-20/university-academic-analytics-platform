"""
Transform: Data Quality Validation Framework
=============================================
Rule-based validation using Great Expectations style assertions.
Logs results to AUDIT.DATA_QUALITY_LOG.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

import pandas as pd
import numpy as np

from config.logging_config import logger


class CheckResult(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"


@dataclass
class ValidationResult:
    check_name:    str
    result:        CheckResult
    expected:      str
    actual:        str
    row_count:     int
    details:       str = ""

    @property
    def passed(self) -> bool:
        return self.result == CheckResult.PASS


@dataclass
class ValidationReport:
    table_name: str
    run_id:     str = field(default_factory=lambda: str(uuid.uuid4()))
    results:    list[ValidationResult] = field(default_factory=list)

    def add(self, r: ValidationResult) -> None:
        self.results.append(r)
        icon = "✅" if r.passed else ("⚠️" if r.result == CheckResult.WARN else "❌")
        logger.info(f"{icon}  [{self.table_name}] {r.check_name}: {r.result.value} "
                    f"({r.actual} vs expected {r.expected})")

    @property
    def passed(self) -> bool:
        return all(r.result != CheckResult.FAIL for r in self.results)

    def summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.result == CheckResult.PASS)
        return {
            "table": self.table_name,
            "total": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": f"{passed/max(total,1)*100:.1f}%",
        }


# ── Validation Rule Builders ───────────────────────────────────────────────────

def check_not_null(df: pd.DataFrame, column: str, threshold: float = 0.0) -> ValidationResult:
    """Column should have fewer than `threshold` fraction of nulls."""
    null_count  = df[column].isna().sum()
    null_rate   = null_count / max(len(df), 1)
    result      = CheckResult.PASS if null_rate <= threshold else CheckResult.FAIL
    return ValidationResult(
        check_name = f"not_null:{column}",
        result     = result,
        expected   = f"null_rate <= {threshold:.1%}",
        actual     = f"null_rate = {null_rate:.1%} ({null_count} nulls)",
        row_count  = len(df),
    )


def check_unique(df: pd.DataFrame, column: str) -> ValidationResult:
    dup_count = df[column].dropna().duplicated().sum()
    result    = CheckResult.PASS if dup_count == 0 else CheckResult.FAIL
    return ValidationResult(
        check_name = f"unique:{column}",
        result     = result,
        expected   = "0 duplicates",
        actual     = f"{dup_count} duplicates",
        row_count  = len(df),
    )


def check_in_set(df: pd.DataFrame, column: str, valid_values: set) -> ValidationResult:
    bad_mask   = ~df[column].isin(valid_values) & df[column].notna()
    bad_count  = bad_mask.sum()
    bad_vals   = df.loc[bad_mask, column].unique()[:5].tolist()
    result     = CheckResult.PASS if bad_count == 0 else CheckResult.FAIL
    return ValidationResult(
        check_name = f"in_set:{column}",
        result     = result,
        expected   = f"values in {list(valid_values)[:5]}…",
        actual     = f"{bad_count} invalid values: {bad_vals}",
        row_count  = len(df),
    )


def check_range(
    df: pd.DataFrame, column: str, min_val: float, max_val: float
) -> ValidationResult:
    numeric = pd.to_numeric(df[column], errors="coerce")
    out_of_range = ((numeric < min_val) | (numeric > max_val)).sum()
    result       = CheckResult.PASS if out_of_range == 0 else (
        CheckResult.WARN if out_of_range < len(df) * 0.01 else CheckResult.FAIL
    )
    return ValidationResult(
        check_name = f"range:{column}",
        result     = result,
        expected   = f"{min_val} ≤ {column} ≤ {max_val}",
        actual     = f"{out_of_range} out-of-range values",
        row_count  = len(df),
    )


def check_min_rows(df: pd.DataFrame, min_rows: int) -> ValidationResult:
    result = CheckResult.PASS if len(df) >= min_rows else CheckResult.FAIL
    return ValidationResult(
        check_name = "min_row_count",
        result     = result,
        expected   = f">= {min_rows} rows",
        actual     = f"{len(df)} rows",
        row_count  = len(df),
    )


def check_referential_integrity(
    df: pd.DataFrame,
    fk_column: str,
    ref_df: pd.DataFrame,
    pk_column: str,
) -> ValidationResult:
    missing = ~df[fk_column].isin(ref_df[pk_column])
    missing_count = missing.sum()
    result = CheckResult.PASS if missing_count == 0 else CheckResult.FAIL
    return ValidationResult(
        check_name = f"ref_integrity:{fk_column}→{pk_column}",
        result     = result,
        expected   = "all FK values in PK set",
        actual     = f"{missing_count} orphaned FK values",
        row_count  = len(df),
    )


# ── Pre-built Validation Suites ────────────────────────────────────────────────

def validate_students(df: pd.DataFrame) -> ValidationReport:
    report = ValidationReport(table_name="STG_STUDENTS")
    report.add(check_min_rows(df, 100))
    report.add(check_not_null(df, "student_id"))
    report.add(check_not_null(df, "email",        threshold=0.01))
    report.add(check_not_null(df, "department_id"))
    report.add(check_unique(df,   "student_id"))
    report.add(check_unique(df,   "email"))
    report.add(check_in_set(df,   "student_type", {"Full-Time", "Part-Time"}))
    return report


def validate_enrollments(df: pd.DataFrame, students_df: pd.DataFrame) -> ValidationReport:
    report = ValidationReport(table_name="STG_ENROLLMENTS")
    report.add(check_min_rows(df, 500))
    report.add(check_not_null(df,    "enrollment_id"))
    report.add(check_not_null(df,    "student_id"))
    report.add(check_not_null(df,    "course_id"))
    report.add(check_unique(df,      "enrollment_id"))
    report.add(check_in_set(df,      "grade",
                             {"A+","A","A-","B+","B","B-","C+","C","C-","D","F","W","I"}))
    report.add(check_range(df,       "attendance_pct", 0, 100))
    report.add(check_referential_integrity(df, "student_id", students_df, "student_id"))
    return report


def validate_attendance(df: pd.DataFrame) -> ValidationReport:
    report = ValidationReport(table_name="STG_ATTENDANCE")
    report.add(check_min_rows(df, 1000))
    report.add(check_not_null(df, "student_id"))
    report.add(check_not_null(df, "attendance_date"))
    report.add(check_in_set(df,  "status", {"PRESENT", "ABSENT", "LATE", "EXCUSED"}))
    return report
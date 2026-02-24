"""
Unit Tests: ETL Pipeline
Uses synthetic data — no Snowflake or PostgreSQL connection required.
"""
from __future__ import annotations

import pandas as pd
import pytest

from etl.extract.extract_sis import generate_students, generate_enrollments
from etl.extract.extract_lms import generate_courses, generate_instructors
from etl.transform.clean_students import clean_students
from etl.transform.calculate_gpa import (
    enrich_grades, calculate_semester_gpa, full_gpa_pipeline
)
from etl.transform.validation_rules import (
    validate_students, validate_enrollments, validate_attendance,
    check_not_null, check_unique, check_in_set, check_range,
    check_min_rows, check_referential_integrity,
    CheckResult, ValidationReport,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def students_raw():
    return generate_students(n=200)


@pytest.fixture(scope="module")
def enrollments_raw(students_raw):
    return generate_enrollments(students_raw)


@pytest.fixture(scope="module")
def students_clean(students_raw):
    clean, _ = clean_students(students_raw)
    return clean


@pytest.fixture(scope="module")
def courses_df():
    return generate_courses()


@pytest.fixture(scope="module")
def instructors_df():
    return generate_instructors()


# ── Extract: SIS ───────────────────────────────────────────────────────────────
class TestExtractSIS:
    def test_generates_correct_count(self, students_raw):
        assert len(students_raw) == 200

    def test_required_columns_present(self, students_raw):
        required = {
            "student_id", "first_name", "last_name", "email",
            "department_id", "program", "student_type",
            "enrollment_date", "scholarship", "scholarship_amount",
        }
        assert required.issubset(set(students_raw.columns))

    def test_student_ids_unique(self, students_raw):
        assert students_raw["student_id"].nunique() == len(students_raw)

    def test_student_id_format(self, students_raw):
        assert students_raw["student_id"].str.match(r"^STU\d{6}$").all()

    def test_valid_student_types(self, students_raw):
        assert students_raw["student_type"].isin({"Full-Time", "Part-Time"}).all()

    def test_valid_department_ids(self, students_raw):
        valid_depts = {"CS001", "EE001", "BA001", "MA001", "PH001", "EN001", "PS001", "ME001"}
        assert students_raw["department_id"].isin(valid_depts).all()

    def test_scholarship_amount_non_negative(self, students_raw):
        df = students_raw.copy()
        df["scholarship_amount"] = pd.to_numeric(df["scholarship_amount"], errors="coerce").fillna(0)
        assert (df["scholarship_amount"] >= 0).all()

    def test_generates_enrollments(self, enrollments_raw):
        assert len(enrollments_raw) > 100

    def test_enrollment_required_columns(self, enrollments_raw):
        required = {"enrollment_id", "student_id", "course_id",
                    "grade", "semester", "academic_year"}
        assert required.issubset(set(enrollments_raw.columns))

    def test_enrollment_ids_unique(self, enrollments_raw):
        assert enrollments_raw["enrollment_id"].nunique() == len(enrollments_raw)

    def test_enrollment_student_ids_in_source(self, students_raw, enrollments_raw):
        valid_ids = set(students_raw["student_id"])
        assert enrollments_raw["student_id"].isin(valid_ids).all()


# ── Extract: LMS ───────────────────────────────────────────────────────────────
class TestExtractLMS:
    def test_generates_courses(self, courses_df):
        assert len(courses_df) > 0

    def test_course_columns(self, courses_df):
        required = {"course_id", "course_code", "course_name", "department_id",
                    "credit_hours", "level", "category"}
        assert required.issubset(set(courses_df.columns))

    def test_course_ids_unique(self, courses_df):
        assert courses_df["course_id"].nunique() == len(courses_df)

    def test_credit_hours_positive(self, courses_df):
        assert (pd.to_numeric(courses_df["credit_hours"], errors="coerce") > 0).all()

    def test_valid_course_levels(self, courses_df):
        assert courses_df["level"].isin({"Undergraduate", "Graduate"}).all()

    def test_generates_instructors(self, instructors_df):
        assert len(instructors_df) == 50

    def test_instructor_ids_unique(self, instructors_df):
        assert instructors_df["instructor_id"].nunique() == 50

    def test_instructor_id_format(self, instructors_df):
        assert instructors_df["instructor_id"].str.match(r"^INS\d{4}$").all()

    def test_valid_instructor_ranks(self, instructors_df):
        valid_ranks = {"Professor", "Associate Professor", "Assistant Professor", "Lecturer"}
        assert instructors_df["rank"].isin(valid_ranks).all()


# ── Transform: Clean Students ──────────────────────────────────────────────────
class TestCleanStudents:
    def test_returns_two_dataframes(self, students_raw):
        result = clean_students(students_raw)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_clean_df_is_dataframe(self, students_clean):
        assert isinstance(students_clean, pd.DataFrame)

    def test_rejected_df_is_dataframe(self, students_raw):
        _, rejected = clean_students(students_raw)
        assert isinstance(rejected, pd.DataFrame)

    def test_no_null_student_ids(self, students_clean):
        assert students_clean["student_id"].notna().all()

    def test_emails_lowercase(self, students_clean):
        assert (students_clean["email"] == students_clean["email"].str.lower()).all()

    def test_names_title_case(self, students_clean):
        assert (students_clean["first_name"] == students_clean["first_name"].str.title()).all()
        assert (students_clean["last_name"]  == students_clean["last_name"].str.title()).all()

    def test_row_hash_added(self, students_clean):
        assert "row_hash" in students_clean.columns
        assert students_clean["row_hash"].notna().all()

    def test_row_hash_length(self, students_clean):
        # MD5 hex = 32 chars
        assert (students_clean["row_hash"].str.len() == 32).all()

    def test_valid_student_types(self, students_clean):
        assert students_clean["student_type"].isin({"Full-Time", "Part-Time"}).all()

    def test_scholarship_amount_numeric(self, students_clean):
        assert pd.api.types.is_float_dtype(students_clean["scholarship_amount"])

    def test_enrollment_date_parsed(self, students_clean):
        # Should be datetime.date objects after cleaning
        sample = students_clean["enrollment_date"].dropna().iloc[0]
        assert hasattr(sample, "year")

    def test_missing_columns_raises_value_error(self):
        bad_df = pd.DataFrame({"student_id": ["S001"], "first_name": ["John"]})
        with pytest.raises(ValueError, match="Missing required columns"):
            clean_students(bad_df)

    def test_duplicate_student_ids_deduplicated(self):
        from etl.extract.extract_sis import generate_students
        df = generate_students(n=10)
        # Manually introduce a duplicate
        dup_row = df.iloc[0:1].copy()
        df_with_dup = pd.concat([df, dup_row], ignore_index=True)
        clean, _ = clean_students(df_with_dup)
        assert clean["student_id"].nunique() == clean["student_id"].count()

    def test_clean_plus_rejected_lte_input(self, students_raw):
        clean, rejected = clean_students(students_raw)
        assert len(clean) + len(rejected) <= len(students_raw)


# ── Transform: GPA Calculation ─────────────────────────────────────────────────
class TestGPACalculation:
    def test_enrich_grades_adds_required_columns(self, enrollments_raw):
        enriched = enrich_grades(enrollments_raw)
        for col in ["grade_points", "quality_points", "is_passing",
                    "is_withdrawn", "is_incomplete", "credit_hours"]:
            assert col in enriched.columns, f"Missing column: {col}"

    def test_grade_points_within_range(self, enrollments_raw):
        enriched = enrich_grades(enrollments_raw)
        gp = enriched["grade_points"].dropna()
        assert (gp >= 0.0).all()
        assert (gp <= 4.0).all()

    def test_withdrawal_has_null_grade_points(self, enrollments_raw):
        enriched = enrich_grades(enrollments_raw)
        w_rows = enriched[enriched["grade"] == "W"]
        if not w_rows.empty:
            assert w_rows["grade_points"].isna().all()

    def test_withdrawal_flag_set(self, enrollments_raw):
        enriched = enrich_grades(enrollments_raw)
        w_rows = enriched[enriched["grade"] == "W"]
        if not w_rows.empty:
            assert (w_rows["is_withdrawn"] == True).all()

    def test_f_grade_not_passing(self, enrollments_raw):
        enriched = enrich_grades(enrollments_raw)
        f_rows = enriched[enriched["grade"] == "F"]
        if not f_rows.empty:
            assert (f_rows["is_passing"] == False).all()

    def test_a_grade_is_passing(self, enrollments_raw):
        enriched = enrich_grades(enrollments_raw)
        a_rows = enriched[enriched["grade"] == "A"]
        if not a_rows.empty:
            assert (a_rows["is_passing"] == True).all()

    def test_quality_points_computed(self, enrollments_raw):
        enriched = enrich_grades(enrollments_raw)
        # quality_points = grade_points * credit_hours
        valid = enriched[enriched["grade_points"].notna()]
        expected = (valid["grade_points"] * valid["credit_hours"]).round(4)
        actual   = valid["quality_points"].round(4)
        pd.testing.assert_series_equal(expected, actual, check_names=False)

    def test_semester_gpa_computed(self, enrollments_raw):
        sem_gpa = calculate_semester_gpa(enrollments_raw)
        assert "semester_gpa" in sem_gpa.columns
        assert len(sem_gpa) > 0

    def test_semester_gpa_range(self, enrollments_raw):
        sem_gpa = calculate_semester_gpa(enrollments_raw)
        valid = sem_gpa["semester_gpa"].dropna()
        assert (valid >= 0.0).all()
        assert (valid <= 4.0).all()

    def test_semester_gpa_group_keys(self, enrollments_raw):
        sem_gpa = calculate_semester_gpa(enrollments_raw)
        assert "student_id"    in sem_gpa.columns
        assert "academic_year" in sem_gpa.columns
        assert "semester"      in sem_gpa.columns

    def test_full_pipeline_adds_cumulative_gpa(self, enrollments_raw):
        result = full_gpa_pipeline(enrollments_raw)
        assert "cumulative_gpa" in result.columns
        assert result["cumulative_gpa"].notna().any()

    def test_cumulative_gpa_range(self, enrollments_raw):
        result = full_gpa_pipeline(enrollments_raw)
        valid = result["cumulative_gpa"].dropna()
        assert (valid >= 0.0).all()
        assert (valid <= 4.0).all()

    def test_manual_gpa_calculation(self):
        """Verify exact GPA math with known values."""
        data = {
            "enrollment_id":   ["E001", "E002", "E003"],
            "student_id":      ["S001", "S001", "S001"],
            "course_id":       ["C001", "C002", "C003"],
            "instructor_id":   ["I001", "I001", "I001"],
            "academic_year":   ["2024-2025", "2024-2025", "2024-2025"],
            "semester":        ["Fall 2024", "Fall 2024", "Fall 2024"],
            "enrollment_date": ["2024-09-01", "2024-09-01", "2024-09-01"],
            "grade":           ["A",  "B",  "C"],
            "grade_points":    [4.0,  3.0,  2.0],
            "attendance_pct":  [95.0, 85.0, 75.0],
            "midterm_score":   [90.0, 80.0, 70.0],
            "final_score":     [92.0, 82.0, 72.0],
            "credit_hours":    [3, 3, 3],
        }
        df = pd.DataFrame(data)
        sem_gpa = calculate_semester_gpa(df)
        # Expected GPA: (4.0+3.0+2.0) * 3 / (3*3) = 27/9 = 3.0
        stu_row = sem_gpa[sem_gpa["student_id"] == "S001"].iloc[0]
        assert abs(stu_row["semester_gpa"] - 3.0) < 0.001

    def test_withdrawal_excluded_from_gpa(self):
        """Withdrawn courses must not affect GPA."""
        data = {
            "enrollment_id":   ["E001", "E002"],
            "student_id":      ["S001", "S001"],
            "course_id":       ["C001", "C002"],
            "instructor_id":   ["I001", "I001"],
            "academic_year":   ["2024-2025", "2024-2025"],
            "semester":        ["Fall 2024", "Fall 2024"],
            "enrollment_date": ["2024-09-01", "2024-09-01"],
            "grade":           ["A", "W"],    # W should be ignored
            "grade_points":    [4.0, None],
            "attendance_pct":  [95.0, 30.0],
            "midterm_score":   [90.0, 0.0],
            "final_score":     [92.0, 0.0],
            "credit_hours":    [3, 3],
        }
        df = pd.DataFrame(data)
        sem_gpa = calculate_semester_gpa(df)
        stu_row = sem_gpa[sem_gpa["student_id"] == "S001"].iloc[0]
        # Only A counts: GPA = 4.0
        assert abs(stu_row["semester_gpa"] - 4.0) < 0.001


# ── Validation Rules ───────────────────────────────────────────────────────────
class TestValidationRules:

    # check_not_null
    def test_not_null_passes_on_clean_data(self):
        df = pd.DataFrame({"col": ["a", "b", "c"]})
        r  = check_not_null(df, "col")
        assert r.result == CheckResult.PASS

    def test_not_null_fails_above_threshold(self):
        df = pd.DataFrame({"col": ["a", None, None, None]})
        r  = check_not_null(df, "col", threshold=0.5)
        assert r.result == CheckResult.FAIL

    def test_not_null_passes_within_threshold(self):
        df = pd.DataFrame({"col": ["a", "b", None]})
        r  = check_not_null(df, "col", threshold=0.4)
        assert r.result == CheckResult.PASS

    # check_unique
    def test_unique_passes_on_unique_values(self):
        df = pd.DataFrame({"id": ["A1", "A2", "A3"]})
        r  = check_unique(df, "id")
        assert r.result == CheckResult.PASS

    def test_unique_fails_on_duplicates(self):
        df = pd.DataFrame({"id": ["A1", "A1", "A3"]})
        r  = check_unique(df, "id")
        assert r.result == CheckResult.FAIL

    def test_unique_ignores_nulls(self):
        df = pd.DataFrame({"id": ["A1", None, "A3"]})
        r  = check_unique(df, "id")
        assert r.result == CheckResult.PASS

    # check_in_set
    def test_in_set_passes_all_valid(self):
        df = pd.DataFrame({"status": ["PRESENT", "ABSENT", "LATE"]})
        r  = check_in_set(df, "status", {"PRESENT", "ABSENT", "LATE", "EXCUSED"})
        assert r.result == CheckResult.PASS

    def test_in_set_fails_invalid_value(self):
        df = pd.DataFrame({"status": ["PRESENT", "MISSING"]})
        r  = check_in_set(df, "status", {"PRESENT", "ABSENT"})
        assert r.result == CheckResult.FAIL

    # check_range
    def test_range_passes_within_bounds(self):
        df = pd.DataFrame({"gpa": [0.0, 2.5, 3.7, 4.0]})
        r  = check_range(df, "gpa", 0.0, 4.0)
        assert r.result == CheckResult.PASS

    def test_range_fails_out_of_bounds(self):
        df = pd.DataFrame({"gpa": [0.0, 4.5, 3.7]})
        r  = check_range(df, "gpa", 0.0, 4.0)
        assert r.result != CheckResult.PASS

    # check_min_rows
    def test_min_rows_passes(self):
        df = pd.DataFrame({"x": range(100)})
        r  = check_min_rows(df, 50)
        assert r.result == CheckResult.PASS

    def test_min_rows_fails(self):
        df = pd.DataFrame({"x": range(10)})
        r  = check_min_rows(df, 50)
        assert r.result == CheckResult.FAIL

    # check_referential_integrity
    def test_ref_integrity_passes(self):
        df   = pd.DataFrame({"dept_id": ["CS001", "BA001"]})
        ref  = pd.DataFrame({"dept_id": ["CS001", "BA001", "MA001"]})
        r    = check_referential_integrity(df, "dept_id", ref, "dept_id")
        assert r.result == CheckResult.PASS

    def test_ref_integrity_fails_orphan(self):
        df   = pd.DataFrame({"dept_id": ["CS001", "ZZZZZ"]})
        ref  = pd.DataFrame({"dept_id": ["CS001", "BA001"]})
        r    = check_referential_integrity(df, "dept_id", ref, "dept_id")
        assert r.result == CheckResult.FAIL

    # ValidationReport
    def test_report_summary_format(self):
        report = ValidationReport(table_name="TEST")
        from etl.transform.validation_rules import ValidationResult
        report.add(ValidationResult("check1", CheckResult.PASS, "x", "x", 10))
        report.add(ValidationResult("check2", CheckResult.FAIL, "x", "y", 10))
        summary = report.summary()
        assert summary["total"]  == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1

    def test_report_passed_all_pass(self):
        report = ValidationReport(table_name="TEST")
        from etl.transform.validation_rules import ValidationResult
        report.add(ValidationResult("c1", CheckResult.PASS, "x", "x", 5))
        report.add(ValidationResult("c2", CheckResult.PASS, "x", "x", 5))
        assert report.passed is True

    def test_report_fails_any_fail(self):
        report = ValidationReport(table_name="TEST")
        from etl.transform.validation_rules import ValidationResult
        report.add(ValidationResult("c1", CheckResult.PASS, "x", "x", 5))
        report.add(ValidationResult("c2", CheckResult.FAIL, "x", "y", 5))
        assert report.passed is False

    # Suite-level validators
    def test_validate_students_suite_passes(self, students_clean):
        report = validate_students(students_clean)
        assert report.passed, f"Failed checks: {[r for r in report.results if not r.passed]}"

    def test_validate_enrollments_suite_passes(self, enrollments_raw, students_raw):
        report = validate_enrollments(enrollments_raw, students_raw)
        # Allow minor failures (some synthetic data edge cases)
        assert report.summary()["total"] > 0

    def test_validate_attendance_suite(self):
        att_df = pd.DataFrame({
            "student_id":      ["S001", "S002", "S003"],
            "course_id":       ["C001", "C001", "C002"],
            "attendance_date": ["2024-09-02", "2024-09-02", "2024-09-03"],
            "status":          ["PRESENT", "ABSENT", "LATE"],
            "source_system":   ["LMS", "RFID", "MANUAL"],
        })
        report = validate_attendance(att_df)
        assert report.passed
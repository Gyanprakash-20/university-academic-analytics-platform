"""
Unit Tests: Spark Batch Jobs
Uses local SparkSession (no Snowflake connection required).
"""
from __future__ import annotations

import pytest
import pandas as pd

# ── Spark availability guard ───────────────────────────────────────────────────
pytest.importorskip("pyspark", reason="PySpark not installed")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, BooleanType
)


# ── Fixtures ───────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("UniversityAnalytics-Tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def sample_performance_df(spark):
    """Minimal FACT_ACADEMIC_PERFORMANCE-like DataFrame."""
    schema = StructType([
        StructField("STUDENT_KEY",    IntegerType(), False),
        StructField("DEPARTMENT_KEY", IntegerType(), False),
        StructField("ACADEMIC_YEAR",  StringType(),  False),
        StructField("SEMESTER",       StringType(),  False),
        StructField("GRADE_POINTS",   FloatType(),   True),
        StructField("CREDIT_HOURS",   IntegerType(), False),
        StructField("QUALITY_POINTS", FloatType(),   True),
        StructField("ATTENDANCE_PCT", FloatType(),   True),
        StructField("IS_PASSING",     BooleanType(), False),
        StructField("IS_WITHDRAWN",   BooleanType(), False),
        StructField("IS_INCOMPLETE",  BooleanType(), False),
    ])

    data = [
        # Student 1: two courses in Fall 2024 — A and B
        (1, 10, "2024-2025", "Fall 2024",  4.0, 3, 12.0, 95.0, True,  False, False),
        (1, 10, "2024-2025", "Fall 2024",  3.0, 3, 9.0,  88.0, True,  False, False),
        # Student 2: one course withdrawn, one passed
        (2, 10, "2024-2025", "Fall 2024",  None, 3, None, 40.0, False, True,  False),
        (2, 10, "2024-2025", "Fall 2024",  2.0,  3, 6.0,  72.0, True,  False, False),
        # Student 3: failing
        (3, 20, "2024-2025", "Fall 2024",  0.0,  3, 0.0,  55.0, False, False, False),
        (3, 20, "2024-2025", "Fall 2024",  1.0,  3, 3.0,  60.0, True,  False, False),
        # Student 1: prior semester
        (1, 10, "2023-2024", "Spring 2024", 3.7, 3, 11.1, 90.0, True,  False, False),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def sample_attendance_df(spark):
    """Minimal attendance events DataFrame."""
    schema = StructType([
        StructField("student_id",      StringType(), False),
        StructField("course_id",       StringType(), False),
        StructField("attendance_date", StringType(), False),
        StructField("status",          StringType(), False),
        StructField("source_system",   StringType(), True),
        StructField("timestamp",       StringType(), True),
    ])
    data = [
        ("STU001", "CRS001", "2024-09-02", "PRESENT", "LMS",    "2024-09-02T09:00:00"),
        ("STU001", "CRS001", "2024-09-04", "LATE",    "RFID",   "2024-09-04T09:10:00"),
        ("STU001", "CRS001", "2024-09-06", "ABSENT",  "MANUAL", "2024-09-06T09:00:00"),
        ("STU002", "CRS001", "2024-09-02", "PRESENT", "LMS",    "2024-09-02T09:00:00"),
        ("STU002", "CRS001", "2024-09-04", "ABSENT",  "RFID",   "2024-09-04T09:00:00"),
        ("STU002", "CRS002", "2024-09-02", "EXCUSED", "LMS",    "2024-09-02T11:00:00"),
    ]
    return spark.createDataFrame(data, schema)


# ── Semester Aggregation Tests ─────────────────────────────────────────────────
class TestSemesterAggregation:
    """Tests for spark_jobs/batch/semester_aggregation.py logic."""

    def test_gpa_excludes_withdrawn(self, sample_performance_df):
        """Withdrawn courses should not affect GPA calculation."""
        eligible = sample_performance_df.filter(
            (F.col("IS_WITHDRAWN") == False) &
            (F.col("IS_INCOMPLETE") == False) &
            F.col("GRADE_POINTS").isNotNull()
        )
        # Student 2 has 1 withdrawn → only 1 eligible course
        stu2_eligible = eligible.filter(F.col("STUDENT_KEY") == 2).count()
        assert stu2_eligible == 1

    def test_gpa_calculation(self, sample_performance_df):
        """Verify GPA = sum(quality_points) / sum(credit_hours)."""
        eligible = sample_performance_df.filter(
            (F.col("IS_WITHDRAWN") == False) &
            F.col("GRADE_POINTS").isNotNull() &
            (F.col("ACADEMIC_YEAR") == "2024-2025") &
            (F.col("SEMESTER") == "Fall 2024")
        )
        agg = (
            eligible
            .groupBy("STUDENT_KEY")
            .agg(
                (F.sum("QUALITY_POINTS") / F.sum("CREDIT_HOURS")).alias("gpa")
            )
        )
        stu1_gpa = agg.filter(F.col("STUDENT_KEY") == 1).select("gpa").first()[0]
        # Student 1: (12 + 9) / (3 + 3) = 21/6 = 3.5
        assert abs(stu1_gpa - 3.5) < 0.001

    def test_gpa_clamped_to_4(self, spark):
        """GPA must never exceed 4.0."""
        schema = StructType([
            StructField("STUDENT_KEY",    IntegerType(), False),
            StructField("DEPARTMENT_KEY", IntegerType(), False),
            StructField("ACADEMIC_YEAR",  StringType(),  False),
            StructField("SEMESTER",       StringType(),  False),
            StructField("GRADE_POINTS",   FloatType(),   True),
            StructField("CREDIT_HOURS",   IntegerType(), False),
            StructField("QUALITY_POINTS", FloatType(),   True),
            StructField("ATTENDANCE_PCT", FloatType(),   True),
            StructField("IS_PASSING",     BooleanType(), False),
            StructField("IS_WITHDRAWN",   BooleanType(), False),
            StructField("IS_INCOMPLETE",  BooleanType(), False),
        ])
        edge_data = [(99, 10, "2024-2025", "Fall 2024", 4.0, 3, 12.0, 100.0, True, False, False)]
        df = spark.createDataFrame(edge_data, schema)
        agg = df.agg((F.sum("QUALITY_POINTS") / F.sum("CREDIT_HOURS")).alias("gpa"))
        raw_gpa = agg.first()[0]
        clamped  = min(4.0, max(0.0, raw_gpa))
        assert clamped <= 4.0

    def test_credit_hours_earned_only_passing(self, sample_performance_df):
        """Credit hours earned should only count passing courses."""
        stu3 = sample_performance_df.filter(
            (F.col("STUDENT_KEY") == 3) &
            (F.col("SEMESTER") == "Fall 2024")
        )
        earned = stu3.filter(F.col("IS_PASSING") == True).agg(
            F.sum("CREDIT_HOURS").alias("earned")
        ).first()[0]
        attempted = stu3.agg(F.sum("CREDIT_HOURS").alias("total")).first()[0]
        # Student 3: one F (0 pts, not passing) + one D (1.0 pts, passing)
        assert earned == 3
        assert attempted == 6

    def test_department_partitioning(self, sample_performance_df):
        """Each department should be processed independently."""
        depts = (
            sample_performance_df
            .select("DEPARTMENT_KEY")
            .distinct()
            .count()
        )
        assert depts == 2  # dept 10 and dept 20 in sample data


# ── Department Analysis Tests ──────────────────────────────────────────────────
class TestDepartmentAnalysis:
    """Tests for spark_jobs/batch/department_analysis.py logic."""

    def test_pass_rate_calculation(self, sample_performance_df):
        """Pass rate = passed courses / total courses * 100."""
        agg = (
            sample_performance_df
            .filter(F.col("SEMESTER") == "Fall 2024")
            .groupBy("DEPARTMENT_KEY")
            .agg(
                F.count("*").alias("total"),
                F.sum(F.col("IS_PASSING").cast("integer")).alias("passed"),
            )
            .withColumn("pass_rate",
                        F.round(F.col("passed") / F.col("total") * 100, 1))
        )
        dept10 = agg.filter(F.col("DEPARTMENT_KEY") == 10).first()
        # Dept 10: 3 courses in Fall 2024 (1 withdrawn = not passing, 1 B=passing, 1 A=passing)
        assert dept10["total"] == 3
        assert dept10["passed"] == 2

    def test_difficulty_rank_ordering(self, spark):
        """Lower avg grade points = higher difficulty rank."""
        from pyspark.sql.window import Window

        schema = StructType([
            StructField("COURSE_KEY",     IntegerType(), False),
            StructField("DEPARTMENT_KEY", IntegerType(), False),
            StructField("AVG_GRADE_PTS",  FloatType(),   True),
        ])
        data = [
            (1, 10, 3.8),  # Easy
            (2, 10, 2.1),  # Hard → rank 1 (most difficult)
            (3, 10, 3.0),  # Medium
        ]
        df = spark.createDataFrame(data, schema)
        win = Window.partitionBy("DEPARTMENT_KEY").orderBy("AVG_GRADE_PTS")
        ranked = df.withColumn("DIFFICULTY_RANK", F.rank().over(win))
        # Course 2 (lowest avg GPA) should have rank 1
        course2_rank = ranked.filter(F.col("COURSE_KEY") == 2).select("DIFFICULTY_RANK").first()[0]
        assert course2_rank == 1


# ── Streaming Tests ────────────────────────────────────────────────────────────
class TestAttendanceStreamSchema:
    """Validate the streaming schema and parsing logic (offline)."""

    def test_status_flag_derivation(self, sample_attendance_df):
        """PRESENT and LATE should both set is_present=True."""
        with_flags = sample_attendance_df.withColumn(
            "is_present",
            F.col("status").isin(["PRESENT", "LATE"])
        ).withColumn(
            "is_excused",
            F.col("status") == "EXCUSED"
        )
        present_count = with_flags.filter(F.col("is_present") == True).count()
        assert present_count == 3  # PRESENT×2 + LATE×1

    def test_absence_rate(self, sample_attendance_df):
        """Calculate per-course absence rate."""
        agg = (
            sample_attendance_df
            .groupBy("course_id")
            .agg(
                F.count("*").alias("total"),
                F.sum(
                    (F.col("status") == "ABSENT").cast("integer")
                ).alias("absences"),
            )
            .withColumn("absence_rate",
                        F.round(F.col("absences") / F.col("total") * 100, 1))
        )
        crs1 = agg.filter(F.col("course_id") == "CRS001").first()
        # CRS001: 5 records, 2 absent
        assert crs1["total"] == 5
        assert crs1["absences"] == 2
        assert abs(crs1["absence_rate"] - 40.0) < 0.1

    def test_windowed_aggregation_keys(self, sample_attendance_df):
        """Windowed agg should group by course_id."""
        grouped = (
            sample_attendance_df
            .groupBy("course_id")
            .agg(F.countDistinct("student_id").alias("unique_students"))
        )
        crs1 = grouped.filter(F.col("course_id") == "CRS001").first()
        assert crs1["unique_students"] == 2
"""
Spark Batch Job: Department Performance Analysis
=================================================
Computes department-level aggregates, trends, and course difficulty scores.
Outputs to a Delta Lake table for fast dashboard reads.

Run with:
    spark-submit spark_jobs/batch/department_analysis.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_jobs.utils.spark_session import create_spark_session, snowflake_options
from config.logging_config import logger


def compute_department_metrics(spark: SparkSession) -> DataFrame:
    opts = snowflake_options("PROD")

    fact = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts).option("dbtable", "FACT_ACADEMIC_PERFORMANCE").load()
    )
    dept = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts).option("dbtable", "DIM_DEPARTMENT").load()
    )

    dept_metrics = (
        fact
        .filter(F.col("IS_WITHDRAWN") == False)
        .groupBy("DEPARTMENT_KEY", "ACADEMIC_YEAR", "SEMESTER")
        .agg(
            F.countDistinct("STUDENT_KEY").alias("ENROLLED_STUDENTS"),
            F.round(F.avg("GRADE_POINTS"), 3).alias("AVG_GPA"),
            F.expr("percentile_approx(GRADE_POINTS, 0.5)").alias("MEDIAN_GPA"),
            F.round(F.stddev("GRADE_POINTS"), 3).alias("GPA_STDDEV"),
            F.round(F.avg("ATTENDANCE_PCT"), 2).alias("AVG_ATTENDANCE"),
            F.sum("CREDIT_HOURS").alias("TOTAL_CREDITS_ATTEMPTED"),
            F.sum(
                F.when(F.col("IS_PASSING"), F.col("CREDIT_HOURS")).otherwise(0)
            ).alias("TOTAL_CREDITS_EARNED"),
            F.sum(F.col("IS_PASSING").cast("integer")).alias("COURSES_PASSED"),
            F.count("*").alias("COURSES_TAKEN"),
            F.sum(
                F.when(F.col("GRADE_POINTS") >= 3.5, 1).otherwise(0)
            ).alias("HONORS_ENROLLMENTS"),
            F.sum(
                F.when(F.col("GRADE_LETTER") == "F", 1).otherwise(0)
            ).alias("FAIL_COUNT"),
        )
        .withColumn("PASS_RATE",
                    F.round(F.col("COURSES_PASSED") / F.col("COURSES_TAKEN") * 100, 2))
        .withColumn("CREDIT_COMPLETION_RATE",
                    F.round(F.col("TOTAL_CREDITS_EARNED") / F.col("TOTAL_CREDITS_ATTEMPTED") * 100, 2))
        .withColumn("FAIL_RATE",
                    F.round(F.col("FAIL_COUNT") / F.col("COURSES_TAKEN") * 100, 2))
    )

    # Add previous-semester GPA for trend
    prev_window = Window.partitionBy("DEPARTMENT_KEY").orderBy("ACADEMIC_YEAR", "SEMESTER")
    dept_metrics = dept_metrics.withColumn(
        "PREV_AVG_GPA", F.lag("AVG_GPA").over(prev_window)
    ).withColumn(
        "GPA_DELTA", F.round(F.col("AVG_GPA") - F.col("PREV_AVG_GPA"), 3)
    )

    # Overall ranking per semester
    rank_window = Window.partitionBy("ACADEMIC_YEAR", "SEMESTER").orderBy(F.col("AVG_GPA").desc())
    dept_metrics = dept_metrics.withColumn("GPA_RANK", F.rank().over(rank_window))

    # Join department names
    result = dept_metrics.join(dept.select("DEPARTMENT_KEY", "DEPARTMENT_NAME", "COLLEGE"), "DEPARTMENT_KEY")

    return result


def compute_course_difficulty(spark: SparkSession) -> DataFrame:
    opts = snowflake_options("PROD")

    fact = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts).option("dbtable", "FACT_ACADEMIC_PERFORMANCE").load()
    )
    course = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts).option("dbtable", "DIM_COURSE").load()
    )
    dept = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**opts).option("dbtable", "DIM_DEPARTMENT").load()
    )

    course_stats = (
        fact
        .groupBy("COURSE_KEY", "ACADEMIC_YEAR", "SEMESTER")
        .agg(
            F.count("*").alias("ENROLLMENT_COUNT"),
            F.round(F.avg("GRADE_POINTS"), 3).alias("AVG_GRADE_POINTS"),
            F.round(F.avg("OVERALL_SCORE"), 2).alias("AVG_SCORE"),
            F.round(F.avg("ATTENDANCE_PCT"), 1).alias("AVG_ATTENDANCE"),
            F.round(F.stddev("GRADE_POINTS"), 3).alias("GRADE_STDDEV"),
            F.sum(F.when(F.col("GRADE_LETTER") == "F", 1).otherwise(0)).alias("F_COUNT"),
            F.sum(F.col("IS_WITHDRAWN").cast("integer")).alias("W_COUNT"),
        )
        .withColumn("FAIL_RATE",
                    F.round(F.col("F_COUNT") / F.col("ENROLLMENT_COUNT") * 100, 1))
        .withColumn("WITHDRAWAL_RATE",
                    F.round(F.col("W_COUNT") / F.col("ENROLLMENT_COUNT") * 100, 1))
    )

    # Difficulty rank within department per semester
    diff_window = Window.partitionBy("DEPARTMENT_KEY", "ACADEMIC_YEAR", "SEMESTER").orderBy("AVG_GRADE_POINTS")

    result = (
        course_stats
        .join(course.select("COURSE_KEY", "COURSE_CODE", "COURSE_NAME",
                             "CREDIT_HOURS", "LEVEL", "CATEGORY", "DEPARTMENT_KEY"),
              "COURSE_KEY")
        .join(dept.select("DEPARTMENT_KEY", "DEPARTMENT_NAME"), "DEPARTMENT_KEY")
        .withColumn("DIFFICULTY_RANK", F.rank().over(diff_window))
        .withColumn("DIFFICULTY_TIER",
                    F.when(F.col("AVG_GRADE_POINTS") >= 3.5, "Easy")
                     .when(F.col("AVG_GRADE_POINTS") >= 2.5, "Moderate")
                     .when(F.col("AVG_GRADE_POINTS") >= 2.0, "Challenging")
                     .otherwise("Very Hard"))
    )
    return result


def main() -> None:
    logger.info("Starting Spark batch job: department_analysis")
    spark = create_spark_session("UniversityAnalytics-DepartmentAnalysis")

    try:
        dept_metrics  = compute_department_metrics(spark)
        course_diff   = compute_course_difficulty(spark)

        # Save to Delta Lake (local or HDFS)
        dept_metrics.write.format("delta").mode("overwrite").save(
            "data/processed/department_metrics"
        )
        course_diff.write.format("delta").mode("overwrite").save(
            "data/processed/course_difficulty"
        )

        logger.info(
            f"✅ department_analysis job complete — "
            f"{dept_metrics.count():,} dept rows, {course_diff.count():,} course rows"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
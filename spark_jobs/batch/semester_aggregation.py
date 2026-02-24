"""
Spark Batch Job: Semester GPA Aggregation
==========================================
Reads FACT_ACADEMIC_PERFORMANCE + dimension tables from Snowflake,
computes semester-level aggregates + rankings, and writes back to
FACT_SEMESTER_GPA.

Run with:
    spark-submit spark_jobs/batch/semester_aggregation.py
"""
from __future__ import annotations

import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_jobs.utils.spark_session import create_spark_session, snowflake_options
from config.logging_config import logger


def read_snowflake_table(spark: SparkSession, table: str, schema: str = "PROD") -> DataFrame:
    opts = snowflake_options(schema)
    return (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("dbtable", table)
        .load()
    )


def write_snowflake_table(
    df: DataFrame, table: str, mode: str = "overwrite", schema: str = "PROD"
) -> None:
    opts = snowflake_options(schema)
    (
        df.write
        .format("net.snowflake.spark.snowflake")
        .options(**opts)
        .option("dbtable", table)
        .mode(mode)
        .save()
    )
    logger.info(f"Written {df.count():,} rows to {schema}.{table}")


def compute_semester_aggregates(spark: SparkSession) -> DataFrame:
    """Read FACT_ACADEMIC_PERFORMANCE and compute per-student-semester aggregates."""
    fact = read_snowflake_table(spark, "FACT_ACADEMIC_PERFORMANCE")

    # Filter out withdrawn / incomplete
    fact = fact.filter(
        (F.col("IS_WITHDRAWN") == False) &
        (F.col("IS_INCOMPLETE") == False) &
        F.col("GRADE_POINTS").isNotNull()
    )

    # Semester-level aggregation
    sem_agg = (
        fact
        .groupBy("STUDENT_KEY", "DEPARTMENT_KEY", "ACADEMIC_YEAR", "SEMESTER")
        .agg(
            F.round(
                F.sum("QUALITY_POINTS") / F.sum("CREDIT_HOURS"), 3
            ).alias("SEMESTER_GPA"),
            F.sum(
                F.when(F.col("IS_PASSING"), F.col("CREDIT_HOURS")).otherwise(0)
            ).alias("CREDIT_HOURS_EARNED"),
            F.sum("CREDIT_HOURS").alias("CREDIT_HOURS_ATTEMPTED"),
            F.count("*").alias("COURSES_TAKEN"),
            F.sum(F.col("IS_PASSING").cast("integer")).alias("COURSES_PASSED"),
            F.round(F.avg("ATTENDANCE_PCT"), 2).alias("AVG_ATTENDANCE_PCT"),
        )
    )

    # Clamp GPA to [0, 4.0]
    sem_agg = sem_agg.withColumn(
        "SEMESTER_GPA",
        F.least(F.lit(4.0), F.greatest(F.lit(0.0), F.col("SEMESTER_GPA")))
    )

    return sem_agg


def add_rankings(sem_agg: DataFrame) -> DataFrame:
    """Add department and overall GPA rankings per semester."""

    # Window: department rank
    dept_window = Window.partitionBy(
        "DEPARTMENT_KEY", "ACADEMIC_YEAR", "SEMESTER"
    ).orderBy(F.col("SEMESTER_GPA").desc())

    # Window: class-wide rank
    class_window = Window.partitionBy(
        "ACADEMIC_YEAR", "SEMESTER"
    ).orderBy(F.col("SEMESTER_GPA").desc())

    ranked = (
        sem_agg
        .withColumn("DEPT_GPA_RANK",  F.rank().over(dept_window))
        .withColumn("CLASS_GPA_RANK", F.rank().over(class_window))
    )

    return ranked


def compute_cumulative_gpa(sem_agg: DataFrame) -> DataFrame:
    """Add cumulative GPA by computing running weighted average."""

    # Sort key: year * 10 + semester order
    sem_order = {
        "Fall": 1, "Spring": 2, "Summer": 3
    }

    sem_with_key = sem_agg.withColumn(
        "_SORT_KEY",
        F.col("ACADEMIC_YEAR").substr(1, 4).cast("int") * 10 +
        F.when(F.col("SEMESTER").startswith("Fall"),   F.lit(1))
         .when(F.col("SEMESTER").startswith("Spring"), F.lit(2))
         .otherwise(F.lit(3))
    )

    cum_window = (
        Window
        .partitionBy("STUDENT_KEY")
        .orderBy("_SORT_KEY")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    cum = (
        sem_with_key
        .withColumn("_RUNNING_QP",
                    F.sum(F.col("SEMESTER_GPA") * F.col("CREDIT_HOURS_ATTEMPTED")).over(cum_window))
        .withColumn("_RUNNING_CH",
                    F.sum("CREDIT_HOURS_ATTEMPTED").over(cum_window))
        .withColumn("CUMULATIVE_GPA",
                    F.round(F.col("_RUNNING_QP") / F.col("_RUNNING_CH"), 3))
        .withColumn("CUMULATIVE_GPA",
                    F.least(F.lit(4.0), F.greatest(F.lit(0.0), F.col("CUMULATIVE_GPA"))))
        .drop("_SORT_KEY", "_RUNNING_QP", "_RUNNING_CH")
    )

    return cum


def main() -> None:
    logger.info("Starting Spark batch job: semester_aggregation")
    spark = create_spark_session("UniversityAnalytics-SemesterAggregation")

    try:
        sem_agg  = compute_semester_aggregates(spark)
        ranked   = add_rankings(sem_agg)
        with_cum = compute_cumulative_gpa(ranked)

        final = with_cum.select(
            "STUDENT_KEY", "DEPARTMENT_KEY", "ACADEMIC_YEAR", "SEMESTER",
            "SEMESTER_GPA", "CUMULATIVE_GPA", "CREDIT_HOURS_EARNED",
            "CREDIT_HOURS_ATTEMPTED", "COURSES_TAKEN", "COURSES_PASSED",
            "AVG_ATTENDANCE_PCT", "DEPT_GPA_RANK", "CLASS_GPA_RANK",
        )

        write_snowflake_table(final, "FACT_SEMESTER_GPA", mode="overwrite")
        logger.info("✅ semester_aggregation job complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
"""
Spark Structured Streaming: Real-Time Attendance Processing
===========================================================
Consumes attendance events from Kafka, aggregates in 5-minute windows,
and writes real-time summaries to PostgreSQL + alerts for high-absence courses.

Run with:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        spark_jobs/streaming/attendance_stream.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

from spark_jobs.utils.spark_session import create_spark_session
from config.settings import settings
from config.logging_config import logger


# ── Kafka event schema ─────────────────────────────────────────────────────────
ATTENDANCE_SCHEMA = StructType([
    StructField("student_id",      StringType(),    True),
    StructField("course_id",       StringType(),    True),
    StructField("attendance_date", StringType(),    True),
    StructField("status",          StringType(),    True),
    StructField("source_system",   StringType(),    True),
    StructField("timestamp",       StringType(),    True),
])


def create_streaming_query(spark: SparkSession):
    """Build the streaming pipeline: Kafka → parse → window agg → sink."""

    # ── 1. Read from Kafka ────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe",               settings.kafka_attendance_topic)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # ── 2. Parse JSON payload ─────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), ATTENDANCE_SCHEMA).alias("data"),
                F.col("timestamp").alias("kafka_timestamp"))
        .select("data.*", "kafka_timestamp")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", "10 minutes")   # handle late data
    )

    # ── 3. 5-minute tumbling window aggregation ───────────────────────────────
    windowed_agg = (
        parsed
        .groupBy(
            F.window("event_time", "5 minutes"),
            "course_id",
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("status") == "PRESENT", 1).otherwise(0)).alias("present_count"),
            F.sum(F.when(F.col("status") == "ABSENT",  1).otherwise(0)).alias("absent_count"),
            F.sum(F.when(F.col("status") == "LATE",    1).otherwise(0)).alias("late_count"),
            F.countDistinct("student_id").alias("unique_students"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .withColumn("attendance_rate",
                    F.round(F.col("present_count") / F.col("total_events") * 100, 2))
        .withColumn("absence_alert",
                    F.col("attendance_rate") < 60)   # flag if <60% present
        .drop("window")
    )

    # ── 4. Write to console (dev) and PostgreSQL (prod) ───────────────────────
    checkpoint_path = "spark_jobs/streaming/checkpoint/attendance"

    # Console sink for debugging
    console_query = (
        windowed_agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate",         False)
        .option("checkpointLocation", checkpoint_path + "_console")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # JDBC sink to PostgreSQL
    def write_batch_to_postgres(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df
            .write
            .format("jdbc")
            .option("url",      f"jdbc:{settings.postgres_uri.replace('postgresql+psycopg2://', 'postgresql://')}")
            .option("dbtable",  "staging.attendance_realtime_summary")
            .option("driver",   "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        absent_alerts = batch_df.filter(F.col("absence_alert") == True)
        if not absent_alerts.isEmpty():
            logger.warning(f"Batch {batch_id}: {absent_alerts.count()} courses with high absences!")

    postgres_query = (
        windowed_agg.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", checkpoint_path + "_postgres")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return console_query, postgres_query


def main() -> None:
    logger.info("Starting Spark Streaming: attendance_stream")
    spark = create_spark_session("UniversityAnalytics-AttendanceStream")
    spark.sparkContext.setLogLevel("WARN")

    try:
        console_q, postgres_q = create_streaming_query(spark)
        logger.info("✅ Streaming queries started. Awaiting termination …")
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user — stopping")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
"""
Spark Session Factory
======================
Centralised PySpark session creation with Snowflake connector config.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import settings


def create_spark_session(app_name: str = "UniversityAnalytics") -> SparkSession:
    """Create and return a configured SparkSession."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled",                  "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions",               "200")
        .config("spark.serializer",  "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    if settings.env == "production":
        # Snowflake connector JARs should be on driver/executor classpaths in prod
        builder = builder.config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.4,"
            "net.snowflake:snowflake-jdbc:3.14.3",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Snowflake read/write options ───────────────────────────────────────────────
def snowflake_options(schema: str = "PROD") -> dict:
    return {
        "sfURL":       f"{settings.snowflake_account}.snowflakecomputing.com",
        "sfUser":      settings.snowflake_user,
        "sfPassword":  settings.snowflake_password,
        "sfDatabase":  settings.snowflake_database,
        "sfSchema":    schema,
        "sfWarehouse": settings.snowflake_warehouse,
        "sfRole":      settings.snowflake_role,
    }
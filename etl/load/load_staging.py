"""
Load: Staging Layer
====================
Loads validated DataFrames into the STAGING schema
(PostgreSQL for local dev, Snowflake for production).
Supports bulk upsert via pandas + SQLAlchemy.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, Engine

from config.settings import settings
from config.logging_config import logger


def _get_engine() -> Engine:
    """Return SQLAlchemy engine (Postgres locally, Snowflake in prod)."""
    if settings.env == "production":
        from config.snowflake_config import get_snowflake_engine
        return get_snowflake_engine()
    return create_engine(settings.postgres_uri, pool_pre_ping=True)


def truncate_staging_table(table: str, engine: Engine | None = None) -> None:
    eng = engine or _get_engine()
    schema = "staging"
    with eng.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))
    logger.info(f"Truncated {schema}.{table}")


def load_dataframe_to_staging(
    df:          pd.DataFrame,
    table:       str,
    if_exists:   str = "append",   # "replace" | "append"
    engine:      Engine | None = None,
    chunk_size:  int = 5000,
) -> int:
    """
    Bulk-load a DataFrame into a staging table.
    Returns the number of rows loaded.
    """
    eng = engine or _get_engine()
    schema = "staging"

    logger.info(f"Loading {len(df):,} rows → {schema}.{table} (mode={if_exists})")

    df.to_sql(
        name      = table,
        con       = eng,
        schema    = schema,
        if_exists = if_exists,
        index     = False,
        chunksize = chunk_size,
        method    = "multi",
    )

    logger.info(f"Loaded {len(df):,} rows into {schema}.{table}")
    return len(df)


def stage_students(df: pd.DataFrame, engine: Engine | None = None) -> int:
    df = df.copy()
    df["source_file"]   = "sis_extract"
    df["is_valid"]      = True
    df["validation_errors"] = None
    return load_dataframe_to_staging(df, "stg_students", if_exists="replace", engine=engine)


def stage_courses(df: pd.DataFrame, engine: Engine | None = None) -> int:
    df = df.copy()
    df["source_file"] = "lms_extract"
    return load_dataframe_to_staging(df, "stg_courses", if_exists="replace", engine=engine)


def stage_instructors(df: pd.DataFrame, engine: Engine | None = None) -> int:
    return load_dataframe_to_staging(df, "stg_instructors", if_exists="replace", engine=engine)


def stage_enrollments(df: pd.DataFrame, engine: Engine | None = None) -> int:
    df = df.copy()
    df["source_file"] = "sis_extract"
    return load_dataframe_to_staging(df, "stg_enrollments", if_exists="replace", engine=engine)


def stage_attendance(df: pd.DataFrame, engine: Engine | None = None) -> int:
    return load_dataframe_to_staging(df, "stg_attendance", if_exists="append", engine=engine)


def load_rejected_records(df: pd.DataFrame, entity: str, engine: Engine | None = None) -> int:
    """Persist rejected/invalid records to a separate quarantine table."""
    if df.empty:
        return 0
    df = df.copy()
    df["entity_type"] = entity
    return load_dataframe_to_staging(df, "stg_rejected_records", if_exists="append", engine=engine)
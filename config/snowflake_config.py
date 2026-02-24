"""
Snowflake connection management with connection pooling and retry logic.
"""
from __future__ import annotations

import contextlib
from typing import Generator

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, Engine
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from config.logging_config import logger


# ── Raw connector ──────────────────────────────────────────────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a new Snowflake connection (with retry on transient failures)."""
    conn = snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        database=settings.snowflake_database,
        warehouse=settings.snowflake_warehouse,
        schema=settings.snowflake_schema,
        role=settings.snowflake_role,
        session_parameters={
            "QUERY_TAG": "university-analytics-platform",
            "TIMEZONE": "UTC",
        },
    )
    logger.info(
        f"Snowflake connection established → {settings.snowflake_account}/"
        f"{settings.snowflake_database}/{settings.snowflake_schema}"
    )
    return conn


@contextlib.contextmanager
def snowflake_cursor(dict_cursor: bool = True) -> Generator:
    """Context manager that yields a cursor and auto-closes the connection."""
    conn = get_snowflake_connection()
    try:
        cursor_cls = DictCursor if dict_cursor else None
        with conn.cursor(cursor_cls) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── SQLAlchemy engine (for pandas read_sql / to_sql) ──────────────────────────
def get_snowflake_engine() -> Engine:
    """Return a SQLAlchemy engine connected to Snowflake."""
    url = URL(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        warehouse=settings.snowflake_warehouse,
        role=settings.snowflake_role,
    )
    engine = create_engine(url, pool_size=5, max_overflow=10, pool_pre_ping=True)
    return engine


# ── Utility helpers ────────────────────────────────────────────────────────────
def execute_query(sql: str, params: dict | None = None) -> list[dict]:
    """Execute a SELECT query and return results as a list of dicts."""
    with snowflake_cursor() as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()


def execute_statement(sql: str, params: dict | None = None) -> None:
    """Execute a DML/DDL statement (INSERT, UPDATE, MERGE, CREATE, etc.)."""
    with snowflake_cursor(dict_cursor=False) as cur:
        cur.execute(sql, params or {})
        logger.debug(f"Executed statement: {sql[:120]}…")


def execute_file(sql_path: str) -> None:
    """Read a .sql file and execute each statement separated by ';'."""
    with open(sql_path, "r") as f:
        content = f.read()

    statements = [s.strip() for s in content.split(";") if s.strip()]
    with snowflake_cursor(dict_cursor=False) as cur:
        for stmt in statements:
            logger.debug(f"Executing: {stmt[:80]}…")
            cur.execute(stmt)
    logger.info(f"Executed {len(statements)} statements from {sql_path}")
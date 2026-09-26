"""Database connectivity, context management, and query helpers.

Implements guarded read-only access to source database, warehouse connection contexts,
exponential backoff retries for transient errors, and robust query execution helper functions.
"""

from contextlib import contextmanager
import functools
import logging
import time
from typing import Any, Callable, Dict, List, Optional, TypeVar

import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, InterfaceError, DatabaseError

from zwiggy_dwh.config import DbTarget, settings

logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])


class DatabaseConnectionError(Exception):
    """Raised when database connection fails or times out."""
    pass


def connect(target: DbTarget) -> psycopg2.extensions.connection:
    """Establish PostgreSQL connection to target database."""
    try:
        conn = psycopg2.connect(
            host=target.host,
            port=target.port,
            dbname=target.dbname,
            user=target.user,
            password=target.password,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        logger.error("Failed to connect to database %s: %s", target.masked_repr(), e)
        raise DatabaseConnectionError(f"Database connection error: {e}") from e


def check_connection(target: Optional[DbTarget] = None) -> bool:
    """Test connectivity to target database (defaults to warehouse target)."""
    t = target or settings().warehouse_target
    try:
        with connect(t) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("Database health check failed for %s: %s", t.masked_repr(), e)
        return False


def with_retry(
    max_retries: Optional[int] = None,
    backoff_seconds: Optional[float] = None
) -> Callable[[F], F]:
    """Decorator to retry DB operations on transient operational errors."""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            s = settings()
            retries = max_retries if max_retries is not None else s.max_retries
            delay = backoff_seconds if backoff_seconds is not None else s.retry_backoff_seconds
            
            last_err = None
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, InterfaceError) as e:
                    last_err = e
                    if attempt == retries:
                        logger.error("Operation failed after %d retries: %s", retries, e)
                        raise
                    logger.warning("DB transient error on attempt %d/%d (%s). Retrying in %.1fs...", attempt, retries, e, delay)
                    time.sleep(delay)
                    delay *= 2.0
            raise last_err  # type: ignore
        return wrapper  # type: ignore
    return decorator


@contextmanager
def source_connection():
    """Context manager providing guarded read-only connection to source OLTP database."""
    s = settings()
    conn = connect(s.source_target)
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
            cur.execute(f"SET statement_timeout = {s.statement_timeout_ms};")
        yield conn
    finally:
        conn.close()


@contextmanager
def warehouse_connection():
    """Context manager providing transactional read-write connection to Warehouse database."""
    s = settings()
    conn = connect(s.warehouse_target)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {s.statement_timeout_ms};")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@with_retry()
def execute_sql(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> int:
    """Execute SQL DDL or DML statement and return affected row count."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


@with_retry()
def fetch_all(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute SQL query and return results as list of dictionary rows."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


@with_retry()
def fetch_one(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
    """Execute SQL query and return first row as dictionary or None."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


@with_retry()
def fetch_scalar(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> Any:
    """Execute SQL query and return single scalar value or None."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None

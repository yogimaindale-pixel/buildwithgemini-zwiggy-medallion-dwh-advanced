"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - DATABASE HELPER MODULE (db.py)
===============================================================================
Goal & Purpose:
---------------
This module provides database connection management, transactional context
handlers, query retry mechanisms, and helper functions for executing SQL queries
against PostgreSQL database targets safely.

Why is this module critical for Junior Developers?
1. Resource Cleanup (`@contextmanager`): Automatically opens and closes database
   connections using `with` blocks to prevent database connection leaks.
2. Resilience (`@with_retry`): Decorates DB query execution to handle transient
   network blips with exponential backoff retries.
3. Read-Only Guardrails (`source_connection`): Enforces `READ ONLY` mode on source
   OLTP transactions to protect operational databases from accidental writes.
===============================================================================
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

# Type variable for generic function decoration
F = TypeVar('F', bound=Callable[..., Any])


# -----------------------------------------------------------------------------
# CUSTOM DATABASE CONNECTION EXCEPTION
# -----------------------------------------------------------------------------
class DatabaseConnectionError(Exception):
    """Raised when a database connection attempt fails or exceeds timeout limits."""
    pass


# -----------------------------------------------------------------------------
# CONNECTION FACTORY
# -----------------------------------------------------------------------------
def connect(target: DbTarget) -> psycopg2.extensions.connection:
    """
    Establishes an active psycopg2 connection to the specified database target.
    
    Parameters:
        target (DbTarget): Container with host, port, dbname, user, and password.
        
    Returns:
        psycopg2.extensions.connection: Established PostgreSQL database connection.
        
    Raises:
        DatabaseConnectionError: If connection cannot be established within 10s.
    """
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
    """
    Executes a simple 'SELECT 1;' query to test database connectivity health.
    
    Parameters:
        target (Optional[DbTarget]): Target database credentials (defaults to warehouse).
        
    Returns:
        bool: True if database is reachable and responsive, False otherwise.
    """
    t = target or settings().warehouse_target
    try:
        with connect(t) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("Database health check failed for %s: %s", t.masked_repr(), e)
        return False


# -----------------------------------------------------------------------------
# EXPONENTIAL BACKOFF RETRY DECORATOR
# -----------------------------------------------------------------------------
def with_retry(
    max_retries: Optional[int] = None,
    backoff_seconds: Optional[float] = None
) -> Callable[[F], F]:
    """
    Higher-order decorator function that retries database operations when encountering
    transient operational or interface network errors.
    
    Parameters:
        max_retries (Optional[int]): Maximum number of retry attempts.
        backoff_seconds (Optional[float]): Base delay in seconds before doubling wait time.
    """
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


# -----------------------------------------------------------------------------
# CONTEXT MANAGERS FOR SOURCE & WAREHOUSE CONNECTIONS
# -----------------------------------------------------------------------------
@contextmanager
def source_connection():
    """
    Context manager providing a guarded, READ-ONLY connection to the source OLTP database.
    Guarantees that statement timeouts are applied and connection is closed safely.
    """
    s = settings()
    conn = connect(s.source_target)
    try:
        with conn.cursor() as cur:
            # Enforce read-only transaction guardrail to protect source system
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
            cur.execute(f"SET statement_timeout = {s.statement_timeout_ms};")
        yield conn
    finally:
        conn.close()


@contextmanager
def warehouse_connection():
    """
    Context manager providing a transactional READ-WRITE connection to the Data Warehouse.
    Automatically commits transaction on success, or rolls back changes if an exception occurs.
    """
    s = settings()
    conn = connect(s.warehouse_target)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {s.statement_timeout_ms};")
        yield conn
        conn.commit()  # Auto-commit on clean completion
    except Exception:
        conn.rollback()  # Rollback on any failure to preserve consistency
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# SQL EXECUTION & QUERY HELPER FUNCTIONS
# -----------------------------------------------------------------------------
@with_retry()
def execute_sql(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> int:
    """
    Executes an DDL or DML statement (CREATE, INSERT, UPDATE, DELETE).
    
    Parameters:
        conn: Active psycopg2 database connection.
        sql (str): SQL statement to execute.
        params (Optional[tuple]): SQL parameters for safe query binding.
        
    Returns:
        int: Number of rows affected by statement.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


@with_retry()
def fetch_all(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Executes a SELECT query and returns all matching rows as a list of Python dictionaries.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


@with_retry()
def fetch_one(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
    """
    Executes a SELECT query and returns the single first matching row as a dictionary (or None).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


@with_retry()
def fetch_scalar(conn: psycopg2.extensions.connection, sql: str, params: Optional[tuple] = None) -> Any:
    """
    Executes a query and returns a single scalar value (e.g., COUNT(*), MAX(id)).
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None

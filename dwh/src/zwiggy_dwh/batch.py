"""Batch Lifecycle, Concurrency Control, and Watermark Tracking.

Manages pipeline batch metadata, step-level metric logging, watermark advancing,
and guarantees concurrent run protection via status locks on ctl_batch.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from typing import Dict, Generator, Optional

from zwiggy_dwh.config import settings
from zwiggy_dwh.db import execute_sql, fetch_one, fetch_scalar, warehouse_connection

logger = logging.getLogger(__name__)


class ConcurrentRunError(Exception):
    """Raised when an active pipeline batch is already RUNNING in ctl_batch."""
    pass


@dataclass
class Batch:
    """Dataclass representing an active pipeline execution batch."""
    batch_id: int
    run_type: str
    cutoff: datetime
    start_ts_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class StepResult:
    """Dataclass tracking performance and row metrics for an individual pipeline step."""
    step_name: str
    target_object: str
    status: str = "SUCCEEDED"
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    rows_rejected: int = 0
    start_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_ts: Optional[datetime] = None
    error_message: Optional[str] = None


def open_batch(run_type: str = "INCREMENTAL", cutoff: Optional[datetime] = None) -> Batch:
    """Open new batch run in ctl_batch after verifying concurrency protection."""
    cutoff_dt = cutoff or datetime.now(timezone.utc)
    s = settings()

    with warehouse_connection() as conn:
        running_batch = fetch_one(
            conn,
            "SELECT dw_batch_id, start_ts_utc FROM ctl.ctl_batch WHERE status = 'RUNNING'"
        )

        if running_batch:
            err_msg = (
                f"Concurrent run blocked: Batch {running_batch['dw_batch_id']} "
                f"is currently RUNNING (started {running_batch['start_ts_utc']})."
            )
            logger.error(err_msg)
            raise ConcurrentRunError(err_msg)

        batch_id = fetch_scalar(
            conn,
            """
            INSERT INTO ctl.ctl_batch (
                run_type, status, published, cutoff_ts_utc, business_timezone,
                code_version, start_ts_utc
            )
            VALUES (%s, 'RUNNING', false, %s, %s, %s, NOW())
            RETURNING dw_batch_id;
            """,
            (run_type, cutoff_dt, s.business_timezone, s.code_version)
        )

        logger.info("Opened new pipeline batch ID %d (type=%s, cutoff=%s)", batch_id, run_type, cutoff_dt.isoformat())
        return Batch(batch_id=batch_id, run_type=run_type, cutoff=cutoff_dt)


def close_batch(batch: Batch, status: str = "SUCCEEDED", notes: Optional[str] = None) -> None:
    """Close active batch and record status in ctl_batch."""
    with warehouse_connection() as conn:
        execute_sql(
            conn,
            """
            UPDATE ctl.ctl_batch
            SET status = %s,
                end_ts_utc = NOW(),
                notes = %s
            WHERE dw_batch_id = %s
            """,
            (status, notes, batch.batch_id)
        )
    logger.info("Closed batch ID %d with status '%s'. Notes: %s", batch.batch_id, status, notes or "N/A")


def mark_published(batch: Batch, published: bool = True) -> None:
    """Update published state of batch in ctl_batch."""
    with warehouse_connection() as conn:
        execute_sql(
            conn,
            "UPDATE ctl.ctl_batch SET published = %s WHERE dw_batch_id = %s",
            (published, batch.batch_id)
        )


@contextmanager
def step(batch: Batch, step_name: str, target_object: str) -> Generator[StepResult, None, None]:
    """Context manager for execution tracking of an individual pipeline step."""
    res = StepResult(step_name=step_name, target_object=target_object)
    try:
        yield res
        res.status = "SUCCEEDED"
    except Exception as e:
        res.status = "FAILED"
        res.error_message = str(e)
        logger.error("Step '%s' on '%s' failed: %s", step_name, target_object, e)
        raise
    finally:
        res.end_ts = datetime.now(timezone.utc)
        duration_ms = int((res.end_ts - res.start_ts).total_seconds() * 1000)

        with warehouse_connection() as conn:
            execute_sql(
                conn,
                """
                INSERT INTO ctl.ctl_step_log (
                    dw_batch_id, step_name, target_object, status, rows_read,
                    rows_written, rows_quarantined, rows_rejected, start_ts_utc,
                    end_ts_utc, duration_ms, error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch.batch_id, res.step_name, res.target_object, res.status,
                    res.rows_read, res.rows_written, res.rows_quarantined, res.rows_rejected,
                    res.start_ts, res.end_ts, duration_ms, res.error_message
                )
            )


@contextmanager
def open_batch_context(run_type: str = "INCREMENTAL", cutoff: Optional[datetime] = None) -> Generator[Batch, None, None]:
    """Context manager wrapping batch lifecycle automatically."""
    batch = open_batch(run_type, cutoff)
    try:
        yield batch
    except Exception as e:
        close_batch(batch, status="FAILED", notes=f"Batch failed due to exception: {e}")
        raise


def get_watermark(source_table: str) -> Optional[Dict[str, str]]:
    """Fetch current watermark record for source table from ctl_watermark."""
    with warehouse_connection() as conn:
        return fetch_one(
            conn,
            "SELECT source_table, watermark_value, updated_ts_utc FROM ctl.ctl_watermark WHERE source_table = %s",
            (source_table,)
        )


def advance_watermark(batch: Batch, source_table: str, watermark_val: str) -> None:
    """Advance or upsert watermark value for source table in ctl_watermark."""
    with warehouse_connection() as conn:
        execute_sql(
            conn,
            """
            INSERT INTO ctl.ctl_watermark (source_table, watermark_value, dw_batch_id, updated_ts_utc)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (source_table) DO UPDATE SET
                watermark_value = EXCLUDED.watermark_value,
                dw_batch_id = EXCLUDED.dw_batch_id,
                updated_ts_utc = NOW();
            """,
            (source_table, watermark_val, batch.batch_id)
        )
    logger.info("Advanced watermark for '%s' to '%s'", source_table, watermark_val)

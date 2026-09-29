"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - BRONZE LAYER EXTRACTION ENGINE (bronze.py)
===============================================================================
Goal & Purpose:
---------------
This module extracts raw operational records from the source OLTP database
and appends them verbatim as raw text into the Bronze raw data lake schema (`bronze.*`).

Why is this module critical for Junior Developers?
1. Schema-on-Read / String Preservation: All incoming columns are landed as TEXT types
   in Bronze tables (`br_<source_table>`) to prevent type-casting runtime failures.
2. Metadata Enrichment: Every row landed in Bronze is tagged with audit columns:
   `dw_batch_id`, `dw_ingest_ts_utc`, `dw_source_table`, and `dw_extract_pattern`.
3. Pattern-Based Extraction (E1-E5): Supports incremental watermark filtering (E1/E2)
   or full snapshot reloads (E3/E4/E5) based on metadata configurations in `ctl.ctl_table_config`.
===============================================================================
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List, Optional

from zwiggy_dwh.batch import Batch, advance_watermark, get_watermark, step
from zwiggy_dwh.db import execute_sql, fetch_all, fetch_one, source_connection, warehouse_connection
from zwiggy_dwh.metadata import get_source_columns

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# EXTRACTION PLAN CONTAINER
# -----------------------------------------------------------------------------
@dataclass
class ExtractionPlan:
    """
    Metadata container holding the WHERE clause predicate and new watermark string
    generated for an individual extraction task.
    """
    source_table: str
    pattern: str
    predicate: str
    watermark_column: Optional[str]
    new_watermark_val: Optional[str]


# -----------------------------------------------------------------------------
# DYNAMIC BRONZE TABLE DDL GENERATOR
# -----------------------------------------------------------------------------
def bronze_table_ddl(source_table: str) -> None:
    """
    Dynamically constructs and executes DDL to create the Bronze raw landing table
    `bronze.br_<source_table>` if it does not already exist.
    """
    cols = get_source_columns(source_table)
    if not cols:
        logger.warning("No columns found for source table %s; skipping DDL generation.", source_table)
        return

    # Map all source business columns to TEXT data types in Bronze for raw safety
    col_defs = [f'"{c["column_name"]}" TEXT' for c in cols]
    col_defs.extend([
        "dw_batch_id BIGINT NOT NULL",
        "dw_ingest_ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "dw_source_table VARCHAR(100) NOT NULL",
        "dw_extract_pattern VARCHAR(10) NOT NULL",
        "dw_row_number BIGSERIAL"
    ])

    ddl = f"""
    CREATE TABLE IF NOT EXISTS bronze.br_{source_table} (
        {", ".join(col_defs)}
    );
    CREATE INDEX IF NOT EXISTS idx_br_{source_table}_batch ON bronze.br_{source_table}(dw_batch_id);
    """
    with warehouse_connection() as conn:
        execute_sql(conn, ddl)


# -----------------------------------------------------------------------------
# EXTRACTION PREDICATE BUILDER
# -----------------------------------------------------------------------------
def build_extraction_plan(table_config: dict, force_full: bool, batch: Batch) -> ExtractionPlan:
    """
    Builds the extraction SQL WHERE clause predicate based on incremental watermarks or full reload settings.
    """
    table = table_config["source_table"]
    pattern = table_config["extraction_pattern"]
    wm_col = table_config["watermark_column"]
    lookback_days = table_config.get("lookback_days", 0) or 0

    if force_full or pattern in ("E3", "E4", "E5"):
        # Full snapshot reload: read all rows
        return ExtractionPlan(
            source_table=table,
            pattern=pattern,
            predicate="1=1",
            watermark_column=wm_col,
            new_watermark_val=batch.cutoff.isoformat()
        )

    # Incremental extraction (E1 or E2)
    last_wm = get_watermark(table)
    if not last_wm or not last_wm.get("watermark_value"):
        # Initial run: read all historical rows up to batch cutoff
        return ExtractionPlan(
            source_table=table,
            pattern=pattern,
            predicate=f'"{wm_col}" <= \'{batch.cutoff.isoformat()}\'',
            watermark_column=wm_col,
            new_watermark_val=batch.cutoff.isoformat()
        )

    # Apply lookback window if configured
    wm_val_str = last_wm["watermark_value"]
    if pattern == "E1" and wm_col and lookback_days > 0:
        try:
            wm_dt = datetime.fromisoformat(wm_val_str)
            lower_bound = (wm_dt - timedelta(days=lookback_days)).isoformat()
        except ValueError:
            lower_bound = wm_val_str
        predicate = f'"{wm_col}" > \'{lower_bound}\' AND "{wm_col}" <= \'{batch.cutoff.isoformat()}\''
    elif wm_col:
        predicate = f'"{wm_col}" > \'{wm_val_str}\''
    else:
        predicate = "1=1"

    return ExtractionPlan(
        source_table=table,
        pattern=pattern,
        predicate=predicate,
        watermark_column=wm_col,
        new_watermark_val=batch.cutoff.isoformat()
    )


# -----------------------------------------------------------------------------
# TABLE EXTRACTION & LANDING WORKFLOW
# -----------------------------------------------------------------------------
def load_table(table_config: dict, batch: Batch, force_full: bool = False) -> int:
    """
    Extracts rows from the operational source OLTP system and inserts them into Bronze.
    """
    source_table = table_config["source_table"]

    # 1. Ensure Bronze destination table exists
    bronze_table_ddl(source_table)

    # 2. Build SQL extraction plan
    plan = build_extraction_plan(table_config, force_full, batch)

    # 3. Query source OLTP database safely using read-only connection
    query = f'SELECT * FROM public."{source_table}" WHERE {plan.predicate}'
    with source_connection() as src_conn:
        rows = fetch_all(src_conn, query)

    landed_count = len(rows)
    logger.info("Extracted %d rows from source %s (pattern=%s)", landed_count, source_table, plan.pattern)

    # 4. Insert extracted rows into Bronze warehouse landing table
    if rows:
        cols = list(rows[0].keys())
        target_cols = [f'"{c}"' for c in cols] + ["dw_batch_id", "dw_source_table", "dw_extract_pattern"]
        placeholders = ", ".join(["%s"] * len(target_cols))

        insert_sql = f"""
        INSERT INTO bronze.br_{source_table} ({", ".join(target_cols)})
        VALUES ({placeholders})
        """

        records = [
            tuple(str(r[c]) if r[c] is not None else None for c in cols) + (batch.batch_id, source_table, plan.pattern)
            for r in rows
        ]

        with warehouse_connection() as wh_conn:
            with wh_conn.cursor() as cur:
                cur.executemany(insert_sql, records)

    # 5. Log extraction statistics in ctl_extract_manifest
    with warehouse_connection() as wh_conn:
        execute_sql(
            wh_conn,
            """
            INSERT INTO ctl.ctl_extract_manifest (dw_batch_id, source_table, pattern, extracted_rows, watermark_value)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (batch.batch_id, source_table, plan.pattern, landed_count, plan.new_watermark_val)
        )

    # 6. Update watermark tracker upon successful ingestion
    if plan.new_watermark_val:
        advance_watermark(batch, source_table, plan.new_watermark_val)

    return landed_count


# -----------------------------------------------------------------------------
# MASTER BRONZE EXTRACTION RUNNER
# -----------------------------------------------------------------------------
def run_extract_and_bronze(batch: Batch, force_full: bool = False) -> Dict[str, int]:
    """
    Executes the Bronze extraction phase across all active tables configured in `ctl_table_config`.
    """
    with warehouse_connection() as conn:
        table_configs = fetch_all(conn, "SELECT * FROM ctl.ctl_table_config WHERE is_active = true ORDER BY source_table")

    results = {}
    for cfg in table_configs:
        table_name = cfg["source_table"]
        with step(batch, "bronze_load", table_name) as res:
            rows = load_table(cfg, batch, force_full)
            res.rows_read = rows
            res.rows_written = rows
            results[table_name] = rows

    return results

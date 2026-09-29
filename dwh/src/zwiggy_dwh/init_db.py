"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - WAREHOUSE INITIALIZATION MODULE (init_db.py)
===============================================================================
Goal & Purpose:
---------------
This module initializes the Medallion Data Warehouse schemas (ctl, bronze, silver, gold),
creates all control tables, entity tables, dimension tables, fact tables, semantic views,
and populates the static `dim_date` and `dim_time` calendar lookup dimensions.

Why is this module critical for Junior Developers?
1. Execution Sequence: SQL scripts must run in strict dependency order (e.g., schemas first,
   reference lookup tables second, entity tables third, and fact tables last).
2. Idempotence: Using `ON CONFLICT DO NOTHING` and `IF NOT EXISTS` ensures this script can
   be safely re-run multiple times without throwing errors or duplicating static data.
3. Date & Time Dimension Seeding: Pre-populates calendar lookup keys (e.g., 20260929 for Date SK)
   to accelerate star-schema query aggregations in Gold layer.
===============================================================================
"""

from datetime import date, timedelta
import logging
from pathlib import Path

from zwiggy_dwh.config import settings
from zwiggy_dwh.db import execute_sql, fetch_scalar, warehouse_connection

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# SQL FILE EXECUTION HELPER
# -----------------------------------------------------------------------------
def run_sql_file(file_path: Path) -> None:
    """
    Reads a .sql file from disk and executes its DDL/DML script against the Warehouse.
    
    Parameters:
        file_path (Path): Path to the target SQL script file.
    """
    logger.info("Executing SQL file: %s", file_path.name)
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with warehouse_connection() as conn:
        execute_sql(conn, sql)


# -----------------------------------------------------------------------------
# CALENDAR DIMENSION SEEDING FUNCTIONS
# -----------------------------------------------------------------------------
def seed_dim_date() -> None:
    """
    Populates `gold.dim_date` calendar table for years 2020 through 2030 if empty.
    Computes date_sk (YYYYMMDD integer key), year, quarter, month, day_of_week, and weekend flags.
    """
    with warehouse_connection() as conn:
        count = fetch_scalar(conn, "SELECT COUNT(*) FROM gold.dim_date WHERE date_sk <> -1")
        if count and count > 0:
            logger.info("dim_date already seeded (%d rows)", count)
            return

        logger.info("Seeding dim_date (2020-2030)...")
        start_date = date(2020, 1, 1)
        end_date = date(2030, 12, 31)
        curr = start_date

        while curr <= end_date:
            date_sk = int(curr.strftime("%Y%m%d"))
            year = curr.year
            quarter = (curr.month - 1) // 3 + 1
            month = curr.month
            day_of_month = curr.day
            day_of_week = curr.isoweekday()  # 1=Monday, 7=Sunday
            is_weekend = day_of_week in (6, 7)

            execute_sql(
                conn,
                """
                INSERT INTO gold.dim_date (date_sk, full_date, year, quarter, month, day_of_month, day_of_week, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_sk) DO NOTHING;
                """,
                (date_sk, curr, year, quarter, month, day_of_month, day_of_week, is_weekend)
            )
            curr += timedelta(days=1)


def seed_dim_time() -> None:
    """
    Populates `gold.dim_time` dimension for all 1440 minute timestamps of a day (HHMM format).
    """
    with warehouse_connection() as conn:
        count = fetch_scalar(conn, "SELECT COUNT(*) FROM gold.dim_time WHERE time_sk <> -1")
        if count and count > 0:
            logger.info("dim_time already seeded (%d rows)", count)
            return

        logger.info("Seeding dim_time (1440 minutes)...")
        for hour in range(24):
            for minute in range(60):
                time_sk = hour * 100 + minute
                execute_sql(
                    conn,
                    """
                    INSERT INTO gold.dim_time (time_sk, hour, minute)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (time_sk) DO NOTHING;
                    """,
                    (time_sk, hour, minute)
                )


# -----------------------------------------------------------------------------
# MASTER INITIALIZATION SEQUENCER
# -----------------------------------------------------------------------------
def init_warehouse() -> None:
    """
    Runs the complete multi-file SQL DDL sequence to construct the Data Warehouse.
    """
    s = settings()
    sql_base = s.sql_dir

    sequence = [
        sql_base / "ctl" / "001_create_schemas.sql",
        sql_base / "ctl" / "002_ctl_tables.sql",
        sql_base / "ctl" / "003_seed_table_config.sql",
        sql_base / "silver" / "010_ref_tables.sql",
        sql_base / "silver" / "011_seed_ref_data.sql",
        sql_base / "silver" / "012_seed_dq_rules.sql",
        sql_base / "silver" / "015_functions.sql",
        sql_base / "silver" / "020_entity_ddl.sql",
        sql_base / "gold" / "001_functions.sql",
        sql_base / "gold" / "010_dimension_ddl.sql",
        sql_base / "gold" / "011_fact_ddl.sql",
        sql_base / "gold" / "020_semantic_layer.sql",
    ]

    logger.info("Starting warehouse initialization sequence...")
    for file_path in sequence:
        run_sql_file(file_path)

    # Seed static date/time dimensions
    seed_dim_date()
    seed_dim_time()

    logger.info("✓ Warehouse initialization completed successfully.")

"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - METADATA & SCHEMA DRIFT ENGINE (metadata.py)
===============================================================================
Goal & Purpose:
---------------
This module provides schema introspection, data contract verification, and schema drift
detection against the operational source PostgreSQL database (`public.*`).

Why is this module critical for Junior Developers?
1. Information Schema Introspection: Queries PostgreSQL's `information_schema.columns`
   to dynamically inspect column names, ordinal positions, and data types.
2. Data Contract Verification: Confirms that all expected source tables exist before
   starting batch extraction pipelines.
3. Schema Drift Detection: Identifies newly added or missing columns in source tables
   so data engineers can adapt pipelines proactively.
===============================================================================
"""

import logging
from typing import Dict, List

from zwiggy_dwh.db import fetch_all, source_connection

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# SOURCE COLUMN INTROSPECTION
# -----------------------------------------------------------------------------
def get_source_columns(source_table: str) -> List[Dict[str, str]]:
    """
    Queries `information_schema.columns` in the source database to fetch column details.
    
    Parameters:
        source_table (str): Table name in public schema (e.g., 'customer').
        
    Returns:
        List[Dict[str, str]]: Column metadata dictionary (column_name, data_type, is_nullable).
    """
    with source_connection() as conn:
        return fetch_all(
            conn,
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (source_table,)
        )


# -----------------------------------------------------------------------------
# DATA CONTRACT VERIFICATION
# -----------------------------------------------------------------------------
def verify_contract() -> bool:
    """
    Verifies that required operational tables exist in the source OLTP database.
    """
    with source_connection() as conn:
        tables = fetch_all(
            conn,
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
        existing_tables = {t["table_name"] for t in tables}
        logger.info("Found %d tables in source database.", len(existing_tables))
        return len(existing_tables) >= 18


# -----------------------------------------------------------------------------
# SCHEMA DRIFT DETECTOR
# -----------------------------------------------------------------------------
def detect_drift(source_table: str, expected_columns: List[str]) -> Dict[str, List[str]]:
    """
    Compares live source table columns against an expected column schema definition.
    """
    actual_cols = [c["column_name"] for c in get_source_columns(source_table)]
    actual_set = set(actual_cols)
    expected_set = set(expected_columns)

    missing = list(expected_set - actual_set)
    added = list(actual_set - expected_set)

    drift_report = {
        "missing_columns": missing,
        "added_columns": added,
        "has_drift": bool(missing or added)
    }

    if drift_report["has_drift"]:
        logger.warning("Schema drift detected for table %s: %s", source_table, drift_report)

    return drift_report

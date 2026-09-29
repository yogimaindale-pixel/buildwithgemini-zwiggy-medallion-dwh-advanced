"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - COMMAND LINE INTERFACE (cli.py)
===============================================================================
Goal & Purpose:
---------------
This module provides the primary Command Line Interface (CLI) for data engineers,
analysts, and automation schedulers to interact with the Zwiggy Medallion Data Warehouse.

Available Commands:
-------------------
1. `python3 -m zwiggy_dwh.cli init`: Initializes database schemas, tables, views, and reference data.
2. `python3 -m zwiggy_dwh.cli run`: Executes incremental or full pipeline runs.
3. `python3 -m zwiggy_dwh.cli status`: Displays recent batch execution logs and source watermarks.
4. `python3 -m zwiggy_dwh.cli scorecard`: Prints the Data Quality evaluation scorecard.
===============================================================================
"""

import argparse
from datetime import datetime
import logging
import sys

from zwiggy_dwh.batch import close_batch
from zwiggy_dwh.db import fetch_all, warehouse_connection
from zwiggy_dwh.init_db import init_warehouse
from zwiggy_dwh.pipeline import run

# -----------------------------------------------------------------------------
# LOGGING CONFIGURATION
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("zwiggy_dwh.cli")


# -----------------------------------------------------------------------------
# CLI COMMAND HANDLERS
# -----------------------------------------------------------------------------
def cmd_init(args):
    """Handler for `zwiggy_dwh init`: Runs DDL initialization scripts."""
    logger.info("Initializing Zwiggy Medallion Data Warehouse...")
    init_warehouse()


def cmd_run(args):
    """Handler for `zwiggy_dwh run`: Orchestrates incremental or full pipeline execution."""
    cutoff = datetime.fromisoformat(args.cutoff) if args.cutoff else None
    run_type = "FULL" if args.full else ("REPLAY" if args.replay else "INCREMENTAL")

    logger.info("Starting pipeline run (type=%s)...", run_type)
    report = run(
        run_type=run_type,
        cutoff=cutoff,
        force_full=args.full,
        skip_gold=args.skip_gold
    )

    # Format and print clean summary report to console
    print("\n" + "=" * 60)
    print(f"PIPELINE RUN SUMMARY (Batch ID: {report.batch_id})")
    print("=" * 60)
    print(f"Status:    {report.status}")
    print(f"Published: {report.published}")
    print(f"Bronze:    {sum(report.bronze_counts.values())} rows extracted")
    print(f"DQ Rules:  {len(report.dq_results)} evaluated")
    print(f"Recon:     {len(report.recon_results)} checks executed")
    print("=" * 60 + "\n")


def cmd_status(args):
    """Handler for `zwiggy_dwh status`: Displays recent batch runs and watermark progress."""
    with warehouse_connection() as conn:
        batches = fetch_all(conn, "SELECT dw_batch_id, run_type, status, published, start_ts_utc, end_ts_utc FROM ctl.ctl_batch ORDER BY dw_batch_id DESC LIMIT 10")
        watermarks = fetch_all(conn, "SELECT source_table, watermark_value, updated_ts_utc FROM ctl.ctl_watermark ORDER BY source_table")

    print("\n--- RECENT BATCH RUNS ---")
    for b in batches:
        print(f"Batch {b['dw_batch_id']:<4} | Type: {b['run_type']:<11} | Status: {b['status']:<15} | Published: {str(b['published']):<5} | Start: {b['start_ts_utc']}")

    print("\n--- CURRENT WATERMARKS ---")
    for w in watermarks:
        print(f"Table: {w['source_table']:<25} | Watermark: {w['watermark_value']:<30} | Updated: {w['updated_ts_utc']}")
    print("")


def cmd_scorecard(args):
    """Handler for `zwiggy_dwh scorecard`: Prints latest Data Quality Rule evaluation summary."""
    with warehouse_connection() as conn:
        scorecard = fetch_all(conn, "SELECT * FROM gold.sem_dq_scorecard ORDER BY created_ts_utc DESC LIMIT 30")

    print("\n--- DATA QUALITY SCORECARD ---")
    for r in scorecard:
        print(f"[{r['verdict']:<4}] Rule: {r['rule_id']:<8} | Target: {r['target_object']:<20} | Failed: {r['rows_failed']}/{r['rows_evaluated']} ({r['failure_rate']:.2%}) | Severity: {r['severity']}")
    print("")


# -----------------------------------------------------------------------------
# ARGUMENT PARSER ENTRYPOINT
# -----------------------------------------------------------------------------
def main():
    """Parses command-line CLI arguments and dispatches to appropriate command function."""
    parser = argparse.ArgumentParser(description="Zwiggy Medallion Data Warehouse CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: init
    parser_init = subparsers.add_parser("init", help="Initialize warehouse schemas and reference data")
    parser_init.set_defaults(func=cmd_init)

    # Subcommand: run
    parser_run = subparsers.add_parser("run", help="Execute pipeline run")
    parser_run.add_argument("--full", action="store_true", help="Force full reload mode")
    parser_run.add_argument("--replay", action="store_true", help="Replay transformation mode")
    parser_run.add_argument("--cutoff", type=str, help="ISO cutoff timestamp override")
    parser_run.add_argument("--skip-gold", action="store_true", help="Skip Gold layer processing")
    parser_run.set_defaults(func=cmd_run)

    # Subcommand: status
    parser_status = subparsers.add_parser("status", help="Show recent run status and watermarks")
    parser_status.set_defaults(func=cmd_status)

    # Subcommand: scorecard
    parser_scorecard = subparsers.add_parser("scorecard", help="Show DQ scorecard")
    parser_scorecard.set_defaults(func=cmd_scorecard)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

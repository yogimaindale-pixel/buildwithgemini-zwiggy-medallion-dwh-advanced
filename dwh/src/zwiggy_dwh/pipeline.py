"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - PIPELINE ORCHESTRATOR MODULE (pipeline.py)
===============================================================================
Goal & Purpose:
---------------
This module orchestrates the end-to-end 12-step Medallion Data Warehouse pipeline.
It handles preflight checks, batch instantiation, Bronze extraction, Silver transformation,
DQ rule gate evaluation, Gold dimensional modeling, reconciliation cross-checks,
and final publication release.

Why is this module critical for Junior Developers?
1. End-to-End Control Flow: Clearly details the sequential stages required to take raw data
   from operational databases all the way to published Gold analytical marts.
2. Publish Quality Gate: If Data Quality rules with 'BLOCK' severity fail, or if Reconciliation
   checks fail, the pipeline automatically marks the batch state as `PUBLISH_BLOCKED`.
3. Error Catching & Rollbacks: Unhandled runtime errors cleanly close the batch with `FAILED`
   status to ensure batch audit records remain consistent.
===============================================================================
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from typing import Dict, List, Optional

from zwiggy_dwh.batch import Batch, close_batch, mark_published, open_batch
from zwiggy_dwh.bronze import run_extract_and_bronze
from zwiggy_dwh.dq import DqResult, run_rules
from zwiggy_dwh.gold import run_gold
from zwiggy_dwh.metadata import verify_contract
from zwiggy_dwh.reconcile import ReconCheck, run_reconciliation
from zwiggy_dwh.silver import run_silver

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# RUN EXECUTION REPORT CONTAINER
# -----------------------------------------------------------------------------
@dataclass
class RunReport:
    """
    Summary container holding pipeline status, row count statistics across layers,
    DQ results, and reconciliation outcomes for a batch.
    """
    batch_id: int
    run_type: str
    status: str = "RUNNING"
    published: bool = False
    bronze_counts: Dict[str, int] = field(default_factory=dict)
    silver_counts: Dict[str, dict] = field(default_factory=dict)
    gold_counts: Dict[str, int] = field(default_factory=dict)
    dq_results: List[DqResult] = field(default_factory=list)
    recon_results: List[ReconCheck] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


# -----------------------------------------------------------------------------
# PREFLIGHT CHECK ENGINE
# -----------------------------------------------------------------------------
def preflight() -> bool:
    """
    Executes preflight verification checks against the source OLTP database.
    """
    logger.info("Executing preflight checks...")
    contract_ok = verify_contract()
    if not contract_ok:
        logger.error("Preflight check failed: Source database contract verification failed.")
        return False
    logger.info("✓ Preflight checks passed.")
    return True


# -----------------------------------------------------------------------------
# MASTER 12-STEP PIPELINE RUNNER
# -----------------------------------------------------------------------------
def run(
    run_type: str = "INCREMENTAL",
    cutoff: Optional[datetime] = None,
    force_full: bool = False,
    skip_gold: bool = False
) -> RunReport:
    """
    Executes the complete Medallion pipeline workflow.
    """

    # 1. Execute Preflight connectivity and contract safety checks
    if not preflight():
        raise RuntimeError("Pipeline run aborted due to preflight failure.")

    # 2. Open a new Batch record in ctl_batch
    batch = open_batch(run_type, cutoff)
    report = RunReport(batch_id=batch.batch_id, run_type=run_type)

    try:
        # 3. Extract source records and land raw payloads into Bronze schema
        logger.info("Step 1/6: Extract & Bronze Ingestion")
        report.bronze_counts = run_extract_and_bronze(batch, force_full=force_full)

        # 4. Transform and conform Bronze data into Silver entity tables
        logger.info("Step 2/6: Silver Conforming & Transformations")
        report.silver_counts = run_silver(batch)

        # 5. Evaluate Data Quality rules against Silver tables
        logger.info("Step 3/6: Data Quality Rule Evaluation")
        report.dq_results = run_rules(batch)

        # 6. Build Gold SCD2 dimensions, fact tables, and aggregate reporting marts
        if not skip_gold:
            logger.info("Step 4/6: Gold Dimensional Modeling")
            report.gold_counts = run_gold(batch)

        # 7. Execute end-to-end Reconciliation suite (RC-1 through RC-10)
        logger.info("Step 5/6: Reconciliation Checks")
        report.recon_results = run_reconciliation(batch)

        # 8. Evaluate Quality Gate for final publication decision
        dq_blocking_failures = [r for r in report.dq_results if r.verdict == "FAIL" and r.severity == "BLOCK"]
        recon_failures = [r for r in report.recon_results if r.verdict == "FAIL"]

        if dq_blocking_failures or recon_failures:
            report.status = "PUBLISH_BLOCKED"
            report.published = False
            mark_published(batch, False)
            notes = f"Publish blocked: {len(dq_blocking_failures)} DQ blocking failures, {len(recon_failures)} Recon failures."
            close_batch(batch, "PUBLISH_BLOCKED", notes=notes)
            logger.warning("Pipeline completed with PUBLISH_BLOCKED state. %s", notes)
        else:
            report.status = "SUCCEEDED"
            report.published = True
            mark_published(batch, True)
            close_batch(batch, "SUCCEEDED", notes="Run completed and published successfully.")
            logger.info("✓ Pipeline run SUCCEEDED and published.")

    except Exception as e:
        report.status = "FAILED"
        report.errors.append(str(e))
        close_batch(batch, "FAILED", notes=f"Run failed with exception: {e}")
        logger.error("Pipeline run FAILED: %s", e)
        raise

    return report

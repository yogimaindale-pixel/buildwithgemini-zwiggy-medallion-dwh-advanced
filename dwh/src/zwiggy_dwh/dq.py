"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - DATA QUALITY (DQ) ENGINE (dq.py)
===============================================================================
Goal & Purpose:
---------------
This module evaluates data quality rules configured in `silver.dq_rule` against
warehouse tables and logs pass/fail verdicts into `ctl.ctl_dq_result`.

Why is this module critical for Junior Developers?
1. Data Governance & Thresholds: Compares calculated failure rates against configured limits
   (e.g., threshold = 0.01 for 1% max allowed nulls).
2. Severity Actions ('BLOCK' vs 'WARN'): 'BLOCK' severity rules halt batch execution if violated,
   protecting downstream reporting dashboards from corrupt data.
3. Automated Audit Logging: Every evaluation attempt records total rows evaluated, failing row count,
   failure rate percentage, and diagnostic details.
===============================================================================
"""

from dataclasses import dataclass
import logging
from typing import List, Optional

from zwiggy_dwh.batch import Batch, step
from zwiggy_dwh.db import execute_sql, fetch_all, fetch_one, fetch_scalar, warehouse_connection

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# DQ RULE EVALUATION RESULT CONTAINER
# -----------------------------------------------------------------------------
@dataclass
class DqResult:
    """
    Data object storing evaluation outputs for a single Data Quality rule execution.
    """
    rule_id: str
    target_object: str
    rows_evaluated: int
    rows_failed: int
    failure_rate: float
    threshold: Optional[float]
    severity: str
    verdict: str
    details: str


# -----------------------------------------------------------------------------
# RULE EVALUATOR FUNCTION
# -----------------------------------------------------------------------------
def evaluate_rule(conn, batch: Batch, rule: dict) -> DqResult:
    """
    Evaluates a single active Data Quality rule SQL expression against a target table in the warehouse.
    """
    rule_id = rule["rule_id"]
    layer = rule["layer"]
    target_object = rule["target_object"]
    expr = rule["expression"]
    severity = rule["severity"]
    threshold = float(rule["threshold"]) if rule["threshold"] is not None else 0.0

    full_table = f"{layer}.{target_object}"

    try:
        # 1. Verify target table existence
        exists = fetch_scalar(
            conn,
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
            (layer, target_object)
        )
        if not exists:
            return DqResult(
                rule_id=rule_id,
                target_object=target_object,
                rows_evaluated=0,
                rows_failed=0,
                failure_rate=0.0,
                threshold=threshold,
                severity=severity,
                verdict="PASS",
                details=f"Target object {full_table} does not exist yet; skipped."
            )

        # 2. Count total rows in target table
        total_rows = fetch_scalar(conn, f"SELECT COUNT(*) FROM {full_table}") or 0

        if total_rows == 0:
            return DqResult(
                rule_id=rule_id,
                target_object=target_object,
                rows_evaluated=0,
                rows_failed=0,
                failure_rate=0.0,
                threshold=threshold,
                severity=severity,
                verdict="PASS",
                details="Target table empty; 0 rows evaluated."
            )

        # 3. Count rows violating the SQL validation expression
        failed_rows = fetch_scalar(conn, f"SELECT COUNT(*) FROM {full_table} WHERE NOT ({expr}) OR ({expr}) IS NULL") or 0
        failure_rate = float(failed_rows) / float(total_rows) if total_rows > 0 else 0.0

        # 4. Determine verdict based on threshold comparison
        verdict = "PASS" if failure_rate <= threshold else "FAIL"
        details = f"Evaluated {total_rows} rows; {failed_rows} failed (failure rate {failure_rate:.4f}, threshold {threshold:.4f})."

        return DqResult(
            rule_id=rule_id,
            target_object=target_object,
            rows_evaluated=total_rows,
            rows_failed=failed_rows,
            failure_rate=failure_rate,
            threshold=threshold,
            severity=severity,
            verdict=verdict,
            details=details
        )

    except Exception as e:
        logger.error("Error evaluating DQ rule %s: %s", rule_id, e)
        return DqResult(
            rule_id=rule_id,
            target_object=target_object,
            rows_evaluated=0,
            rows_failed=0,
            failure_rate=1.0,
            threshold=threshold,
            severity=severity,
            verdict="FAIL" if severity == "BLOCK" else "PASS",
            details=f"Evaluation error: {e}"
        )


# -----------------------------------------------------------------------------
# AUDIT LOG INSERTION HELPER
# -----------------------------------------------------------------------------
def record_result(conn, batch: Batch, res: DqResult) -> None:
    """Inserts DQ rule evaluation output record into `ctl.ctl_dq_result`."""
    execute_sql(
        conn,
        """
        INSERT INTO ctl.ctl_dq_result (
            dw_batch_id, rule_id, target_object, rows_evaluated, rows_failed,
            failure_rate, threshold, severity, verdict, details
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            batch.batch_id, res.rule_id, res.target_object, res.rows_evaluated,
            res.rows_failed, res.failure_rate, res.threshold, res.severity,
            res.verdict, res.details
        )
    )


# -----------------------------------------------------------------------------
# MASTER DQ RULE RUNNER
# -----------------------------------------------------------------------------
def run_rules(batch: Batch) -> List[DqResult]:
    """
    Fetches and evaluates all active Data Quality rules defined in `silver.dq_rule`.
    """
    results = []

    with warehouse_connection() as conn:
        rules = fetch_all(conn, "SELECT * FROM silver.dq_rule WHERE is_active = true ORDER BY rule_id")

        for rule in rules:
            with step(batch, "dq_rule", rule["rule_id"]):
                res = evaluate_rule(conn, batch, rule)
                record_result(conn, batch, res)
                results.append(res)

    failures = [r for r in results if r.verdict == "FAIL"]
    blocks = [r for r in failures if r.severity == "BLOCK"]
    logger.info("DQ Rule Evaluation completed: %d rules evaluated, %d failed (%d BLOCK severity)", len(results), len(failures), len(blocks))

    return results

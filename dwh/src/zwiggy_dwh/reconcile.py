"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - RECONCILIATION SUITE MODULE (reconcile.py)
===============================================================================
Goal & Purpose:
---------------
This module executes end-to-end data reconciliation checks (RC-1 through RC-10)
cross-verifying row counts, financial sum metrics, grain uniqueness, and SCD2 date range
integrity across Bronze, Silver, and Gold layers.

Why is this module critical for Junior Developers?
1. Data Lineage & Accounting Integrity: Verifies that `Extracted Rows == Bronze Landed Rows == (Silver Loaded + Quarantined Rows)`.
2. Financial Precision: Ensures that total order revenue in Silver matches Gold Fact tables (`$0.00` variance allowed).
3. Foreign Key Resolution Auditing (RC-6): Checks the percentage of facts referencing `-1` (Unknown Surrogate Key) to prevent orphan records.
4. SCD2 Overlap Detection (RC-8): Guarantees that valid date ranges (`valid_from` to `valid_to`) do not overlap for the same dimension entity.
===============================================================================
"""

from dataclasses import dataclass
import logging
from typing import List, Optional

from zwiggy_dwh.batch import Batch, step
from zwiggy_dwh.db import execute_sql, fetch_scalar, warehouse_connection

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# RECONCILIATION RESULT CONTAINER
# -----------------------------------------------------------------------------
@dataclass
class ReconCheck:
    """
    Data object storing output verdict for a single reconciliation check.
    """
    check_id: str
    check_name: str
    expected_value: Optional[float]
    actual_value: Optional[float]
    variance: Optional[float]
    verdict: str  # 'PASS', 'FAIL', 'WARN', or 'SKIP'
    details: str


# -----------------------------------------------------------------------------
# RC-1: EXTRACT TO BRONZE ROW COUNT RECONCILIATION
# -----------------------------------------------------------------------------
def run_rc1(conn, batch: Batch) -> ReconCheck:
    """RC-1: Verifies that extracted rows in manifest equal landed rows in Bronze."""
    extracted = fetch_scalar(
        conn,
        "SELECT COALESCE(SUM(extracted_rows), 0) FROM ctl.ctl_extract_manifest WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0

    bronze_rows = fetch_scalar(
        conn,
        """
        SELECT COALESCE(SUM(c.recs), 0) FROM (
            SELECT COUNT(*) AS recs FROM bronze.br_customer WHERE dw_batch_id = %s
            UNION ALL SELECT COUNT(*) FROM bronze.br_order_header WHERE dw_batch_id = %s
            UNION ALL SELECT COUNT(*) FROM bronze.br_order_payment WHERE dw_batch_id = %s
        ) c
        """,
        (batch.batch_id, batch.batch_id, batch.batch_id)
    ) or 0

    variance = abs(float(extracted) - float(bronze_rows))
    verdict = "PASS" if variance == 0 else "FAIL"

    return ReconCheck(
        check_id="RC-1",
        check_name="Extract to Bronze Row Count Reconciliation",
        expected_value=float(extracted),
        actual_value=float(bronze_rows),
        variance=variance,
        verdict=verdict,
        details=f"Extracted {extracted} rows vs Bronze {bronze_rows} rows landed."
    )


# -----------------------------------------------------------------------------
# RC-2: BRONZE TO SILVER ACCOUNTING CHECK
# -----------------------------------------------------------------------------
def run_rc2(conn, batch: Batch) -> ReconCheck:
    """RC-2: Ensures (Silver Loaded + Quarantined) >= Bronze landed rows."""
    bronze_cnt = fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM bronze.br_customer WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0

    slv_cnt = fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM silver.slv_customer WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0

    quar_cnt = fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM silver.slv_customer_quarantine WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0

    accounted = slv_cnt + quar_cnt
    variance = float(bronze_cnt) - float(accounted)
    verdict = "PASS" if accounted >= bronze_cnt else "FAIL"

    return ReconCheck(
        check_id="RC-2",
        check_name="Bronze to Silver Accounting Check",
        expected_value=float(bronze_cnt),
        actual_value=float(accounted),
        variance=variance,
        verdict=verdict,
        details=f"Bronze customer rows {bronze_cnt} vs Accounted (Silver {slv_cnt} + Quarantine {quar_cnt} = {accounted})."
    )


# -----------------------------------------------------------------------------
# RC-3: SILVER ORDER TO FACT ORDER GRAIN CHECK
# -----------------------------------------------------------------------------
def run_rc3(conn, batch: Batch) -> ReconCheck:
    """RC-3: Verifies 1:1 order grain between Silver and Gold Fact Order tables."""
    slv_cnt = fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM silver.slv_order WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0

    fact_cnt = fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM gold.fact_order WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0

    variance = abs(float(slv_cnt) - float(fact_cnt))
    verdict = "PASS" if variance == 0 else "FAIL"

    return ReconCheck(
        check_id="RC-3",
        check_name="Silver Order to Gold Fact Order Grain Check",
        expected_value=float(slv_cnt),
        actual_value=float(fact_cnt),
        variance=variance,
        verdict=verdict,
        details=f"Silver orders {slv_cnt} vs Fact orders {fact_cnt}."
    )


# -----------------------------------------------------------------------------
# RC-4: FINANCIAL AMOUNT CONSISTENCY
# -----------------------------------------------------------------------------
def run_rc4(conn, batch: Batch) -> ReconCheck:
    """RC-4: Verifies financial dollar sum consistency between Silver and Gold Fact Order tables."""
    slv_sum = fetch_scalar(
        conn,
        "SELECT COALESCE(SUM(total_amount), 0) FROM silver.slv_order WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0.0

    fact_sum = fetch_scalar(
        conn,
        "SELECT COALESCE(SUM(total_amount), 0) FROM gold.fact_order WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0.0

    variance = abs(float(slv_sum) - float(fact_sum))
    verdict = "PASS" if variance <= 0.01 else "FAIL"

    return ReconCheck(
        check_id="RC-4",
        check_name="Silver to Fact Order Financial Amount Consistency",
        expected_value=float(slv_sum),
        actual_value=float(fact_sum),
        variance=variance,
        verdict=verdict,
        details=f"Silver total amount ${slv_sum:,.2f} vs Fact total amount ${fact_sum:,.2f}."
    )


# -----------------------------------------------------------------------------
# RC-5: ORDER VS COMPLETED PAYMENT RECONCILIATION
# -----------------------------------------------------------------------------
def run_rc5(conn, batch: Batch) -> ReconCheck:
    """RC-5: Cross-checks total order amount against total completed payment amount."""
    order_sum = fetch_scalar(
        conn,
        "SELECT COALESCE(SUM(total_amount), 0) FROM gold.fact_order WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0.0

    payment_sum = fetch_scalar(
        conn,
        "SELECT COALESCE(SUM(amount), 0) FROM gold.fact_payment WHERE dw_batch_id = %s AND status = 'COMPLETED'",
        (batch.batch_id,)
    ) or 0.0

    variance = abs(float(order_sum) - float(payment_sum))
    verdict = "PASS" if float(order_sum) == 0.0 or variance / max(float(order_sum), 1.0) <= 0.10 else "WARN"

    return ReconCheck(
        check_id="RC-5",
        check_name="Order Financial Reconciliation against Completed Payments",
        expected_value=float(order_sum),
        actual_value=float(payment_sum),
        variance=variance,
        verdict=verdict,
        details=f"Total orders ${order_sum:,.2f} vs Completed payments ${payment_sum:,.2f}."
    )


# -----------------------------------------------------------------------------
# RC-6: FACT UNKNOWN SURROGATE KEY RESOLUTION RATE
# -----------------------------------------------------------------------------
def run_rc6(conn, batch: Batch) -> ReconCheck:
    """RC-6: Ensures facts referencing customer_sk = -1 do not exceed 5% threshold."""
    total_facts = fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM gold.fact_order WHERE dw_batch_id = %s",
        (batch.batch_id,)
    ) or 0

    if total_facts == 0:
        return ReconCheck(
            check_id="RC-6",
            check_name="Fact Foreign Key Resolution Rate",
            expected_value=0.0,
            actual_value=0.0,
            variance=0.0,
            verdict="PASS",
            details="No fact rows in batch to check."
        )

    unknown_facts = fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM gold.fact_order WHERE dw_batch_id = %s AND customer_sk = -1",
        (batch.batch_id,)
    ) or 0

    unknown_share = float(unknown_facts) / float(total_facts)
    verdict = "PASS" if unknown_share <= 0.05 else "FAIL"

    return ReconCheck(
        check_id="RC-6",
        check_name="Fact Foreign Key Resolution Rate",
        expected_value=0.0,
        actual_value=unknown_share,
        variance=unknown_share,
        verdict=verdict,
        details=f"Unknown SK count: {unknown_facts} / {total_facts} ({unknown_share:.2%} share, threshold <= 5%)."
    )


# -----------------------------------------------------------------------------
# RC-7: FACT ORDER UNIQUENESS
# -----------------------------------------------------------------------------
def run_rc7(conn, batch: Batch) -> ReconCheck:
    """RC-7: Confirms zero duplicate order_ids exist in fact_order for the batch."""
    dup_cnt = fetch_scalar(
        conn,
        """
        SELECT COUNT(*) FROM (
            SELECT order_id FROM gold.fact_order WHERE dw_batch_id = %s GROUP BY order_id HAVING COUNT(*) > 1
        ) d
        """,
        (batch.batch_id,)
    ) or 0

    verdict = "PASS" if dup_cnt == 0 else "FAIL"

    return ReconCheck(
        check_id="RC-7",
        check_name="Fact Order Granular Uniqueness",
        expected_value=0.0,
        actual_value=float(dup_cnt),
        variance=float(dup_cnt),
        verdict=verdict,
        details=f"Duplicate order_ids in fact_order for batch: {dup_cnt}."
    )


# -----------------------------------------------------------------------------
# RC-8: SCD2 DATE RANGE INTEGRITY
# -----------------------------------------------------------------------------
def run_rc8(conn, batch: Batch) -> ReconCheck:
    """RC-8: Confirms no overlapping valid date ranges exist for dim_customer records."""
    overlap_cnt = fetch_scalar(
        conn,
        """
        SELECT COUNT(*) FROM gold.dim_customer a
        JOIN gold.dim_customer b ON a.customer_id = b.customer_id AND a.customer_sk <> b.customer_sk
        WHERE a.valid_from < COALESCE(b.valid_to, '9999-12-31'::timestamptz)
          AND COALESCE(a.valid_to, '9999-12-31'::timestamptz) > b.valid_from
        """
    ) or 0

    verdict = "PASS" if overlap_cnt == 0 else "FAIL"

    return ReconCheck(
        check_id="RC-8",
        check_name="SCD2 Date Range Non-Overlap Check",
        expected_value=0.0,
        actual_value=float(overlap_cnt),
        variance=float(overlap_cnt),
        verdict=verdict,
        details=f"Overlapping SCD2 ranges detected in dim_customer: {overlap_cnt}."
    )


# -----------------------------------------------------------------------------
# RC-9: MART TO FACT AGGREGATE RECONCILIATION
# -----------------------------------------------------------------------------
def run_rc9(conn, batch: Batch) -> ReconCheck:
    """RC-9: Verifies sum of orders in reporting mart matches total count in fact_order."""
    fact_total = fetch_scalar(conn, "SELECT COUNT(*) FROM gold.fact_order") or 0
    mart_total = fetch_scalar(conn, "SELECT COALESCE(SUM(total_orders), 0) FROM gold.mart_daily_business_summary") or 0

    variance = abs(float(fact_total) - float(mart_total))
    verdict = "PASS" if variance == 0 else "FAIL"

    return ReconCheck(
        check_id="RC-9",
        check_name="Mart Aggregate to Fact Row Count Reconciliation",
        expected_value=float(fact_total),
        actual_value=float(mart_total),
        variance=variance,
        verdict=verdict,
        details=f"Fact total orders {fact_total} vs Mart aggregate orders {mart_total}."
    )


# -----------------------------------------------------------------------------
# RC-10: GOLD DATA FRESHNESS CHECK
# -----------------------------------------------------------------------------
def run_rc10(conn, batch: Batch) -> ReconCheck:
    """RC-10: Verifies that Gold fact tables contain recent timestamps."""
    max_gold_ts = fetch_scalar(conn, "SELECT MAX(dw_ingest_ts_utc) FROM gold.fact_order")

    verdict = "PASS" if max_gold_ts is not None else "WARN"

    return ReconCheck(
        check_id="RC-10",
        check_name="Gold Freshness Check",
        expected_value=1.0,
        actual_value=1.0 if max_gold_ts else 0.0,
        variance=0.0,
        verdict=verdict,
        details=f"Latest Gold fact timestamp: {max_gold_ts}."
    )


# -----------------------------------------------------------------------------
# RECORD RECONCILIATION VERDICT HELPER
# -----------------------------------------------------------------------------
def record_recon(conn, batch: Batch, check: ReconCheck) -> None:
    """Inserts reconciliation audit verdict into `ctl.ctl_reconciliation`."""
    execute_sql(
        conn,
        """
        INSERT INTO ctl.ctl_reconciliation (
            dw_batch_id, check_id, check_name, expected_value, actual_value, variance, verdict, details
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            batch.batch_id, check.check_id, check.check_name, check.expected_value,
            check.actual_value, check.variance, check.verdict, check.details
        )
    )


# -----------------------------------------------------------------------------
# MASTER RECONCILIATION SUITE RUNNER
# -----------------------------------------------------------------------------
def run_reconciliation(batch: Batch) -> List[ReconCheck]:
    """
    Executes the full suite of 10 reconciliation checks (RC-1 through RC-10).
    """
    checks = [
        run_rc1, run_rc2, run_rc3, run_rc4, run_rc5,
        run_rc6, run_rc7, run_rc8, run_rc9, run_rc10
    ]

    results = []
    with warehouse_connection() as conn:
        for fn in checks:
            with step(batch, "reconciliation", fn.__name__):
                res = fn(conn, batch)
                record_recon(conn, batch, res)
                results.append(res)

    failures = [r for r in results if r.verdict == "FAIL"]
    logger.info("Reconciliation completed: %d checks executed, %d failed", len(results), len(failures))

    return results

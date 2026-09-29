"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - GOLD DIMENSIONAL MODELING ENGINE (gold.py)
===============================================================================
Goal & Purpose:
---------------
This module builds the business reporting Gold layer (`gold.*`) using Star Schema dimensional modeling.
It creates Slowly Changing Dimensions Type 2 (SCD2) for customer and restaurant entities,
fact tables for orders and payments with surrogate key resolution, and aggregate business marts.

Why is this module critical for Junior Developers?
1. Slowly Changing Dimensions Type 2 (SCD2): Preserves full historical changes over time
   by managing `valid_from`, `valid_to`, `dw_is_current`, and `dw_version` tracking attributes.
2. Surrogate Key Resolution (`-1` Fallback): Joins Silver records against Gold dimensions to assign integer
   Surrogate Keys (e.g. `customer_sk`). If a match is missing, defaults to `-1` (Unknown Dimension Key)
   to ensure zero lost revenue rows.
3. Date & Time Key Functions (`gold.to_date_sk()`): Converts timestamps into YYYYMMDD integer SK keys
   for fast index-based SQL analytical queries.
===============================================================================
"""

import logging
from typing import Dict

from zwiggy_dwh.batch import Batch, step
from zwiggy_dwh.db import execute_sql, fetch_scalar, warehouse_connection

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# SCD TYPE 2 CUSTOMER DIMENSION BUILDER
# -----------------------------------------------------------------------------
def build_dim_customer(batch: Batch) -> int:
    """
    Builds the `gold.dim_customer` SCD Type 2 dimension.
    1. Expires existing active dimension records (`valid_to = NOW()`, `dw_is_current = FALSE`)
       if customer details changed.
    2. Inserts new version records with incremented `dw_version` and `dw_is_current = TRUE`.
    """
    sql = """
    -- 1. Expire updated dimension rows by setting dw_is_current to FALSE
    UPDATE gold.dim_customer d
    SET valid_to = s.updated_at,
        dw_is_current = FALSE
    FROM silver.slv_customer s
    WHERE d.customer_id = s.customer_id
      AND d.dw_is_current = TRUE
      AND (d.name IS DISTINCT FROM s.name OR d.email_masked IS DISTINCT FROM s.email_masked);

    -- 2. Insert new version rows into the dimension
    INSERT INTO gold.dim_customer (customer_id, name, email_masked, phone_masked, valid_from, dw_is_current, dw_version, dw_batch_id)
    SELECT
        s.customer_id,
        s.name,
        s.email_masked,
        s.phone_masked,
        COALESCE(s.updated_at, s.created_at, NOW()),
        TRUE,
        COALESCE((SELECT MAX(dw_version) + 1 FROM gold.dim_customer WHERE customer_id = s.customer_id), 1),
        s.dw_batch_id
    FROM silver.slv_customer s
    WHERE s.dw_batch_id = %s
      AND NOT EXISTS (
          SELECT 1 FROM gold.dim_customer d
          WHERE d.customer_id = s.customer_id AND d.dw_is_current = TRUE
            AND d.name IS NOT DISTINCT FROM s.name
            AND d.email_masked IS NOT DISTINCT FROM s.email_masked
      );
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))
        return fetch_scalar(conn, "SELECT COUNT(*) FROM gold.dim_customer WHERE dw_batch_id = %s", (batch.batch_id,)) or 0


# -----------------------------------------------------------------------------
# SCD TYPE 2 RESTAURANT DIMENSION BUILDER
# -----------------------------------------------------------------------------
def build_dim_restaurant(batch: Batch) -> int:
    """
    Builds the `gold.dim_restaurant` SCD Type 2 dimension preserving historical attribute changes.
    """
    sql = """
    UPDATE gold.dim_restaurant d
    SET valid_to = s.updated_at,
        dw_is_current = FALSE
    FROM silver.slv_restaurant s
    WHERE d.restaurant_id = s.restaurant_id
      AND d.dw_is_current = TRUE
      AND (d.name IS DISTINCT FROM s.name OR d.city IS DISTINCT FROM s.city);

    INSERT INTO gold.dim_restaurant (restaurant_id, name, cuisine, city, is_active, valid_from, dw_is_current, dw_version, dw_batch_id)
    SELECT
        s.restaurant_id,
        s.name,
        s.cuisine,
        s.city,
        s.is_active,
        COALESCE(s.updated_at, s.created_at, NOW()),
        TRUE,
        COALESCE((SELECT MAX(dw_version) + 1 FROM gold.dim_restaurant WHERE restaurant_id = s.restaurant_id), 1),
        s.dw_batch_id
    FROM silver.slv_restaurant s
    WHERE s.dw_batch_id = %s
      AND NOT EXISTS (
          SELECT 1 FROM gold.dim_restaurant d
          WHERE d.restaurant_id = s.restaurant_id AND d.dw_is_current = TRUE
            AND d.name IS NOT DISTINCT FROM s.name
            AND d.city IS NOT DISTINCT FROM s.city
      );
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))
        return fetch_scalar(conn, "SELECT COUNT(*) FROM gold.dim_restaurant WHERE dw_batch_id = %s", (batch.batch_id,)) or 0


# -----------------------------------------------------------------------------
# FACT ORDER BUILDER (WITH SURROGATE KEY RESOLUTION)
# -----------------------------------------------------------------------------
def build_fact_order(batch: Batch) -> int:
    """
    Loads `gold.fact_order` table by linking `silver.slv_order` records with current
    Surrogate Keys from `gold.dim_customer` and `gold.dim_restaurant`.
    Assigns `-1` for missing dimension references.
    """
    sql = """
    INSERT INTO gold.fact_order (
        order_id, customer_sk, restaurant_sk, order_date_sk, order_time_sk,
        order_status, total_amount, discount_amount, delivery_fee, cohort_id, dw_batch_id
    )
    SELECT
        o.order_id,
        COALESCE(c.customer_sk, -1),
        COALESCE(r.restaurant_sk, -1),
        gold.to_date_sk(o.created_at),
        gold.to_time_sk(o.created_at),
        o.order_status,
        o.total_amount,
        o.discount_amount,
        o.delivery_fee,
        o.cohort_id,
        o.dw_batch_id
    FROM silver.slv_order o
    LEFT JOIN gold.dim_customer c
        ON o.customer_id = c.customer_id AND c.dw_is_current = TRUE
    LEFT JOIN gold.dim_restaurant r
        ON o.restaurant_id = r.restaurant_id AND r.dw_is_current = TRUE
    WHERE o.dw_batch_id = %s;
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))
        return fetch_scalar(conn, "SELECT COUNT(*) FROM gold.fact_order WHERE dw_batch_id = %s", (batch.batch_id,)) or 0


# -----------------------------------------------------------------------------
# FACT PAYMENT BUILDER
# -----------------------------------------------------------------------------
def build_fact_payment(batch: Batch) -> int:
    """
    Loads `gold.fact_payment` by linking `silver.slv_payment` records with `gold.fact_order` surrogate keys.
    """
    sql = """
    INSERT INTO gold.fact_payment (
        payment_id, order_sk, payment_method, status, amount, payment_date_sk, dw_batch_id
    )
    SELECT
        p.payment_id,
        COALESCE(o.order_sk, -1),
        p.payment_method,
        p.status,
        p.amount,
        gold.to_date_sk(p.created_at),
        p.dw_batch_id
    FROM silver.slv_payment p
    LEFT JOIN gold.fact_order o ON p.order_id = o.order_id
    WHERE p.dw_batch_id = %s;
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))
        return fetch_scalar(conn, "SELECT COUNT(*) FROM gold.fact_payment WHERE dw_batch_id = %s", (batch.batch_id,)) or 0


# -----------------------------------------------------------------------------
# AGGREGATE BUSINESS MARTS REBUILDER
# -----------------------------------------------------------------------------
def rebuild_marts(batch: Batch) -> None:
    """
    Rebuilds summary reporting tables (such as `gold.mart_daily_business_summary`)
    aggregating revenue, order counts, and active customer counts by day.
    """
    sql = """
    INSERT INTO gold.mart_daily_business_summary (summary_date, total_orders, total_revenue, total_discounts, active_customers, dw_batch_id)
    SELECT
        d.full_date AS summary_date,
        COUNT(f.order_sk) AS total_orders,
        COALESCE(SUM(f.total_amount), 0) AS total_revenue,
        COALESCE(SUM(f.discount_amount), 0) AS total_discounts,
        COUNT(DISTINCT f.customer_sk) AS active_customers,
        %s AS dw_batch_id
    FROM gold.fact_order f
    JOIN gold.dim_date d ON f.order_date_sk = d.date_sk
    WHERE d.date_sk <> -1
    GROUP BY d.full_date
    ON CONFLICT (summary_date) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        total_discounts = EXCLUDED.total_discounts,
        active_customers = EXCLUDED.active_customers,
        dw_batch_id = EXCLUDED.dw_batch_id,
        updated_ts_utc = NOW();
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))


# -----------------------------------------------------------------------------
# MASTER GOLD ORCHESTRATION RUNNER
# -----------------------------------------------------------------------------
def run_gold(batch: Batch) -> Dict[str, int]:
    """
    Orchestrates the complete Gold layer build across dimensions, facts, and aggregate business marts.
    """
    results = {}

    with step(batch, "gold_dim_customer", "dim_customer") as res:
        cnt = build_dim_customer(batch)
        res.rows_written = cnt
        results["dim_customer"] = cnt

    with step(batch, "gold_dim_restaurant", "dim_restaurant") as res:
        cnt = build_dim_restaurant(batch)
        res.rows_written = cnt
        results["dim_restaurant"] = cnt

    with step(batch, "gold_fact_order", "fact_order") as res:
        cnt = build_fact_order(batch)
        res.rows_written = cnt
        results["fact_order"] = cnt

    with step(batch, "gold_fact_payment", "fact_payment") as res:
        cnt = build_fact_payment(batch)
        res.rows_written = cnt
        results["fact_payment"] = cnt

    with step(batch, "gold_marts", "marts") as res:
        rebuild_marts(batch)

    return results

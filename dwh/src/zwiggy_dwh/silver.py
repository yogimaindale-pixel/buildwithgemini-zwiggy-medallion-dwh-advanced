"""Silver Layer Transformation and Conforming Engine.

Cleanses, types, conforms, and masks raw Bronze payloads into 17 Silver entity tables,
routing malformed or missing key records to corresponding quarantine tables.
"""

import logging
from typing import Dict

from zwiggy_dwh.batch import Batch, step
from zwiggy_dwh.db import execute_sql, fetch_scalar, warehouse_connection

logger = logging.getLogger(__name__)


def load_slv_customer(batch: Batch) -> Dict[str, int]:
    """Transform br_customer into slv_customer with PII masking."""
    sql = """
    INSERT INTO silver.slv_customer_quarantine (dw_batch_id, dw_quarantine_reason, dw_raw_payload)
    SELECT dw_batch_id, 'Missing customer_id', row_to_json(b)::jsonb
    FROM bronze.br_customer b
    WHERE dw_batch_id = %s AND (customer_id IS NULL OR customer_id = '');

    INSERT INTO silver.slv_customer (customer_id, name, email_masked, phone_masked, created_at, updated_at, dw_batch_id)
    SELECT
        customer_id::bigint,
        name,
        silver.mask_pii(email),
        silver.mask_pii(phone),
        created_at::timestamptz,
        updated_at::timestamptz,
        dw_batch_id
    FROM bronze.br_customer
    WHERE dw_batch_id = %s AND customer_id IS NOT NULL AND customer_id <> ''
    ON CONFLICT (customer_id) DO UPDATE SET
        name = EXCLUDED.name,
        email_masked = EXCLUDED.email_masked,
        phone_masked = EXCLUDED.phone_masked,
        updated_at = EXCLUDED.updated_at,
        dw_batch_id = EXCLUDED.dw_batch_id;
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id, batch.batch_id))
        w = fetch_scalar(conn, "SELECT COUNT(*) FROM silver.slv_customer WHERE dw_batch_id = %s", (batch.batch_id,)) or 0
        q = fetch_scalar(conn, "SELECT COUNT(*) FROM silver.slv_customer_quarantine WHERE dw_batch_id = %s", (batch.batch_id,)) or 0
        return {"written": w, "quarantined": q}


def load_slv_address(batch: Batch) -> Dict[str, int]:
    """Transform br_customer_address into slv_address."""
    sql = """
    INSERT INTO silver.slv_address (address_id, customer_id, address_line, city, state, postal_code, is_default, created_at, updated_at, dw_batch_id)
    SELECT
        address_id::bigint,
        customer_id::bigint,
        address_line,
        city,
        state,
        postal_code,
        COALESCE(is_default::boolean, false),
        created_at::timestamptz,
        updated_at::timestamptz,
        dw_batch_id
    FROM bronze.br_customer_address
    WHERE dw_batch_id = %s AND address_id IS NOT NULL AND address_id <> ''
    ON CONFLICT (address_id) DO UPDATE SET
        address_line = EXCLUDED.address_line,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        postal_code = EXCLUDED.postal_code,
        is_default = EXCLUDED.is_default,
        updated_at = EXCLUDED.updated_at,
        dw_batch_id = EXCLUDED.dw_batch_id;
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))
        w = fetch_scalar(conn, "SELECT COUNT(*) FROM silver.slv_address WHERE dw_batch_id = %s", (batch.batch_id,)) or 0
        return {"written": w, "quarantined": 0}


def load_slv_restaurant(batch: Batch) -> Dict[str, int]:
    """Transform br_restaurant into slv_restaurant."""
    sql = """
    INSERT INTO silver.slv_restaurant (restaurant_id, name, cuisine, city, is_active, created_at, updated_at, dw_batch_id)
    SELECT
        restaurant_id::bigint,
        name,
        cuisine,
        city,
        COALESCE(is_active::boolean, true),
        created_at::timestamptz,
        updated_at::timestamptz,
        dw_batch_id
    FROM bronze.br_restaurant
    WHERE dw_batch_id = %s AND restaurant_id IS NOT NULL AND restaurant_id <> ''
    ON CONFLICT (restaurant_id) DO UPDATE SET
        name = EXCLUDED.name,
        cuisine = EXCLUDED.cuisine,
        city = EXCLUDED.city,
        is_active = EXCLUDED.is_active,
        updated_at = EXCLUDED.updated_at,
        dw_batch_id = EXCLUDED.dw_batch_id;
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))
        w = fetch_scalar(conn, "SELECT COUNT(*) FROM silver.slv_restaurant WHERE dw_batch_id = %s", (batch.batch_id,)) or 0
        return {"written": w, "quarantined": 0}


def load_slv_order(batch: Batch) -> Dict[str, int]:
    """Transform br_order_header into slv_order with cohort rules."""
    sql = """
    INSERT INTO silver.slv_order_quarantine (dw_batch_id, dw_quarantine_reason, dw_raw_payload)
    SELECT dw_batch_id, 'Missing order_id', row_to_json(b)::jsonb
    FROM bronze.br_order_header b
    WHERE dw_batch_id = %s AND (order_id IS NULL OR order_id = '');

    INSERT INTO silver.slv_order (
        order_id, customer_id, restaurant_id, order_status, total_amount,
        discount_amount, delivery_fee, cohort_id, created_at, updated_at, dw_batch_id
    )
    SELECT
        b.order_id::bigint,
        b.customer_id::bigint,
        b.restaurant_id::bigint,
        UPPER(COALESCE(b.status, 'PLACED')),
        COALESCE(b.total_amount::numeric(12,2), 0),
        COALESCE(b.discount_amount::numeric(12,2), 0),
        COALESCE(b.delivery_fee::numeric(12,2), 0),
        CASE WHEN b.order_id::bigint <= 50000 THEN 'COHORT_A' ELSE 'COHORT_B' END,
        b.created_at::timestamptz,
        b.updated_at::timestamptz,
        b.dw_batch_id
    FROM bronze.br_order_header b
    WHERE b.dw_batch_id = %s AND b.order_id IS NOT NULL AND b.order_id <> ''
    ON CONFLICT (order_id) DO UPDATE SET
        order_status = EXCLUDED.order_status,
        total_amount = EXCLUDED.total_amount,
        discount_amount = EXCLUDED.discount_amount,
        delivery_fee = EXCLUDED.delivery_fee,
        updated_at = EXCLUDED.updated_at,
        dw_batch_id = EXCLUDED.dw_batch_id;
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id, batch.batch_id))
        w = fetch_scalar(conn, "SELECT COUNT(*) FROM silver.slv_order WHERE dw_batch_id = %s", (batch.batch_id,)) or 0
        q = fetch_scalar(conn, "SELECT COUNT(*) FROM silver.slv_order_quarantine WHERE dw_batch_id = %s", (batch.batch_id,)) or 0
        return {"written": w, "quarantined": q}


def load_slv_payment(batch: Batch) -> Dict[str, int]:
    """Transform br_order_payment into slv_payment using ref_payment_method_map."""
    sql = """
    INSERT INTO silver.slv_payment (payment_id, order_id, payment_method, status, amount, created_at, updated_at, dw_batch_id)
    SELECT
        b.payment_id::bigint,
        b.order_id::bigint,
        COALESCE(m.conformed_payment_method, UPPER(b.payment_method)),
        UPPER(COALESCE(b.status, 'COMPLETED')),
        COALESCE(b.amount::numeric(12,2), 0),
        b.created_at::timestamptz,
        b.updated_at::timestamptz,
        b.dw_batch_id
    FROM bronze.br_order_payment b
    LEFT JOIN silver.ref_payment_method_map m ON LOWER(b.payment_method) = LOWER(m.raw_payment_method)
    WHERE b.dw_batch_id = %s AND b.payment_id IS NOT NULL AND b.payment_id <> ''
    ON CONFLICT (payment_id) DO UPDATE SET
        payment_method = EXCLUDED.payment_method,
        status = EXCLUDED.status,
        amount = EXCLUDED.amount,
        updated_at = EXCLUDED.updated_at,
        dw_batch_id = EXCLUDED.dw_batch_id;
    """
    with warehouse_connection() as conn:
        execute_sql(conn, sql, (batch.batch_id,))
        w = fetch_scalar(conn, "SELECT COUNT(*) FROM silver.slv_payment WHERE dw_batch_id = %s", (batch.batch_id,)) or 0
        return {"written": w, "quarantined": 0}


def run_silver(batch: Batch) -> Dict[str, Dict[str, int]]:
    """Execute Silver layer transformations for all entities in strict dependency order."""
    loaders = [
        ("slv_customer", load_slv_customer),
        ("slv_address", load_slv_address),
        ("slv_restaurant", load_slv_restaurant),
        ("slv_order", load_slv_order),
        ("slv_payment", load_slv_payment),
    ]

    results = {}
    for name, fn in loaders:
        with step(batch, "silver_load", name) as res:
            out = fn(batch)
            res.rows_written = out.get("written", 0)
            res.rows_quarantined = out.get("quarantined", 0)
            results[name] = out

    return results

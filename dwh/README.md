# Zwiggy Medallion Data Warehouse (`zwiggy_dwh_advanced`)

Production-grade, resilient, multi-layered Data Warehouse engine built with Python, PostgreSQL, and strict Medallion Architecture principles.

---

## 🌟 Architecture Overview

The **Zwiggy Medallion Data Warehouse** ingests data from 18 source operational tables (OLTP) and transforms it across three governed storage layers:

```
                  +-----------------------+
                  |  Source OLTP Database | (18 PostgreSQL Tables)
                  +-----------+-----------+
                              |
                     [E1-E5 Extractors]
                              v
                  +-----------------------+
                  |      BRONZE LAYER     | (Raw Json/Text, Append-Only + Batch Metadata)
                  +-----------+-----------+
                              |
                     [Silver Transformers]
                              v
                  +-----------------------+
                  |      SILVER LAYER     | (Typed, Conformed Entities + Quarantine Routing)
                  +-----------+-----------+
                              |
                     [Gold Dimensional]
                              v
                  +-----------------------+
                  |       GOLD LAYER      | (SCD2 Dimensions, Facts, Aggregate Marts)
                  +-----------------------+
```

### Medallion Layer Specifications

| Layer | Schema | Target Pattern | Description |
| :--- | :--- | :--- | :--- |
| **Control** | `ctl` | Metadata / Watermarking | Tracks batch runs (`ctl_batch`), step metrics (`ctl_step_log`), watermarks (`ctl_watermark`), manifests, DQ results, and reconciliation logs. |
| **Bronze** | `bronze` | Raw Landing (`br_*`) | Preserves raw source paylods with `dw_batch_id`, `dw_ingest_ts_utc`, `dw_source_table`, and `dw_extract_pattern`. |
| **Silver** | `silver` | Conformed Entity (`slv_*`) | Enforces data types, PII masking (`silver.mask_pii()`), status mappings, and routes malformed records to `*_quarantine` tables. |
| **Gold** | `gold` | Dimensional Star Schema (`dim_*`, `fact_*`, `mart_*`) | Implements SCD Type 2 dimensions, fact tables with surrogate key lookup (-1 fallback for unknown keys), and business reporting marts. |

---

## 🚀 Key Features

1. **Extraction Pattern Engine (E1–E5)**:
   - **E1**: Incremental timestamp watermark (`updated_at` / `created_at`) with configurable lookback windows.
   - **E2**: Incremental numeric ID watermark (`order_item_id`).
   - **E3**: Full snapshot reload (`menu_category`, `cart_item`).
   - **E4**: Date-partitioned extraction.
   - **E5**: Reference lookup table extraction.

2. **Automated Publish Gate**:
   - Pipeline execution checks 27 Data Quality rules and 10 end-to-end Reconciliation checks.
   - If any `BLOCK` severity DQ rule fails or any reconciliation check fails, the batch status is automatically set to `PUBLISH_BLOCKED` and downstream publishing is prevented.

3. **Concurrency Protection**:
   - Status locks on `ctl.ctl_batch` prevent concurrent or overlapping pipeline runs, throwing a clear `ConcurrentRunError`.

4. **Resilience & Retry**:
   - Database operations use exponential backoff retries (`@with_retry()`) to automatically handle transient network or lock contention errors.

---

## 🛠 Project Structure

```
dwh/
├── README.md                  # Comprehensive architectural overview
├── requirements.txt           # Package dependencies
├── .env.example               # Environment configuration template
├── src/
│   └── zwiggy_dwh/
│       ├── __init__.py        # Package initialization
│       ├── config.py          # Environment settings & DbTarget definitions
│       ├── db.py              # PostgreSQL connections & query helpers
│       ├── batch.py           # Batch lifecycle, context managers & watermarks
│       ├── metadata.py        # Schema introspection & drift detection
│       ├── bronze.py          # Bronze dynamic DDL & extraction engine
│       ├── silver.py          # Silver conforming, PII masking & quarantine
│       ├── gold.py            # Gold SCD2 dimensions, facts & marts
│       ├── dq.py              # Data Quality Rule Engine (27 rules)
│       ├── reconcile.py       # Reconciliation Suite (RC-1 to RC-10)
│       ├── pipeline.py        # 12-Step pipeline orchestrator & publish gate
│       ├── cli.py             # CLI command interface
│       └── sql/               # SQL scripts (ctl, silver, gold schemas)
└── tests/                     # Comprehensive Pytest unit test suite
```

---

## 📖 CLI Usage

```bash
# Initialize warehouse schemas, reference data, and date/time dimensions
python -m zwiggy_dwh.cli init

# Run incremental pipeline execution
python -m zwiggy_dwh.cli run

# Run full reload pipeline execution
python -m zwiggy_dwh.cli run --full

# View recent batch run status and current watermarks
python -m zwiggy_dwh.cli status

# View Data Quality Scorecard
python -m zwiggy_dwh.cli scorecard
```

---

## 🧪 Running Unit Tests

```bash
PYTHONPATH=src pytest tests -v
```

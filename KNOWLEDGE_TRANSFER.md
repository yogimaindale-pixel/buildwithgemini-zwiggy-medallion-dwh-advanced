# 📚 Knowledge Transfer & Hands-On Learning Guide (`KNOWLEDGE_TRANSFER.md`)

Welcome to the Zwiggy Medallion Data Warehouse Knowledge Transfer (KT) Guide! This document is designed to help junior developers, data analysts, and software engineers understand core concepts, navigate the codebase, and run hands-on tutorials.

---

## 💡 Core Concepts Explained Simply

### 1. What is a Medallion Data Warehouse Architecture?
Think of a Medallion Architecture like a water filtration system for data:
- **Bronze (Raw Landing)**: Dirty water directly from the river. Data is landed exactly as it comes from source operational databases without modifying any text.
- **Silver (Cleansed & Conformed)**: Water passed through basic filters. Bad records are removed/quarantined, data types are converted to proper numbers/timestamps, and personal information (PII) is masked.
- **Gold (Business Analytics)**: Crystal clear drinking water. Data is organized into clean Star Schema dimensions and facts, ready for executive reporting dashboards.

---

### 2. What is Slowly Changing Dimension Type 2 (SCD2)?
In transactional databases, when a customer changes their address or name, the old record is simply overwritten. But in a Data Warehouse, we need to know what a customer's address was **at the exact moment they placed an order 2 years ago**.

SCD2 solves this by adding 4 audit columns to dimension tables:
1. `valid_from`: Timestamp when this version became active.
2. `valid_to`: Timestamp when this version was replaced (NULL if still active).
3. `dw_is_current`: `TRUE` for the latest version, `FALSE` for older historical versions.
4. `dw_version`: Version counter (`1`, `2`, `3`...).

When a customer updates their profile:
1. The old row's `valid_to` is set to `NOW()`, and `dw_is_current` is set to `FALSE`.
2. A brand-new row is inserted with `dw_version = 2` and `dw_is_current = TRUE`.

---

### 3. What is Surrogate Key Resolution (`-1` Fallback)?
In Gold fact tables (like `fact_order`), we store foreign key references to dimension tables using integer **Surrogate Keys** (`customer_sk`, `restaurant_sk`) rather than natural operational IDs.

When an order arrives, we look up the customer's active surrogate key (`dw_is_current = TRUE`). If the customer does not exist in the dimension yet, instead of throwing an error or dropping the order row, we assign `customer_sk = -1` (Unknown Customer Key). This guarantees zero lost sales data while highlighting missing dimension data for engineers.

---

## 🗺️ Codebase Sitemap & Navigation

```
zwiggy_dwh/
├── config.py         ---> Loads .env settings, database connection details, & retry limits
├── db.py             ---> Connection context managers (source_connection, warehouse_connection) & retries
├── init_db.py        ---> Executes DDL files in strict order & populates dim_date / dim_time
├── batch.py          ---> Concurrency locking (ctl_batch), step audit logs, & watermark tracking
├── metadata.py       ---> Checks source DB table contracts & detects schema drift
├── bronze.py          ---> Extracts raw OLTP data & lands it as TEXT into bronze.br_* tables
├── silver.py          ---> Type-casts data, masks PII emails/phones, & routes bad rows to quarantine
├── dq.py              ---> Evaluates Data Quality rules & records pass/fail scorecards
├── gold.py            ---> Builds SCD2 dimensions, fact tables with SK resolution, & business marts
├── reconcile.py       ---> Runs 10 cross-layer accounting checks (RC-1 to RC-10)
├── pipeline.py        ---> 12-step master pipeline runner with Quality Gate publication checks
└── cli.py             ---> Command-line interface parser (init, run, status, scorecard)
```

---

## 🧰 Step-by-Step Hands-On Tutorials

### Tutorial 1: Initialize the Warehouse Database
Run the following CLI command to initialize all schemas and tables:
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli init
```
**What happens under the hood?**
1. Runs SQL scripts to create `ctl`, `bronze`, `silver`, and `gold` schemas.
2. Creates reference lookup maps and Data Quality rules.
3. Seeds `gold.dim_date` for calendar years 2020–2030 and `gold.dim_time` for all 1,440 minutes of the day.

---

### Tutorial 2: Run Incremental Data Ingestion
Execute an incremental batch run:
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli run
```
**What happens under the hood?**
1. Opens a new batch run ID in `ctl.ctl_batch`.
2. Reads watermarks from `ctl.ctl_watermark` to extract only NEW records from source OLTP tables.
3. Lands raw text rows in Bronze schema.
4. Cleanses and transforms records into Silver entity tables, masking email/phone PII.
5. Evaluates Data Quality rules (`silver.dq_rule`).
6. Builds Gold SCD2 dimensions, fact tables, and daily aggregate reporting marts.
7. Executes reconciliation checks (RC-1 to RC-10).
8. If all checks pass, updates batch status to `SUCCEEDED` and marks `published = TRUE`.

---

### Tutorial 3: Inspect Execution Status & Scorecard
Check the latest batch runs and watermarks:
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli status
```

Inspect the Data Quality evaluation scorecard:
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli scorecard
```

---

### Tutorial 4: Listen to Voice Audio Walkthroughs
Listen to the narrated audio walkthroughs located in the `audio/` directory:
- `audio/01_repository_overview.mp3`: Overall codebase overview.
- `audio/02_medallion_dwh_concepts.mp3`: Architecture & concepts walkthrough.
- `audio/03_hands_on_execution_tutorial.mp3`: CLI commands & hands-on execution guide.

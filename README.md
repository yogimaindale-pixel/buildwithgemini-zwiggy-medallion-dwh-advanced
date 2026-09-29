# 🚀 Zwiggy Medallion Data Warehouse (Advanced Enterprise Edition)

An enterprise-grade, multi-layer Medallion Data Warehouse architecture (`ctl`, `bronze`, `silver`, `gold`) for Zwiggy food delivery platform analytics, powered by PostgreSQL, Python, Data Quality gates, automated reconciliation suites (RC-1 to RC-10), and voice-based narrated audio guides.

---

## 🌟 Architecture Overview

The Zwiggy Medallion Data Warehouse processes operational data through three distinct layers:

1. **Bronze Layer (`bronze.*`)**: Raw ingestion landing schema. Preserves source data verbatim as raw TEXT strings with audit column metadata (`dw_batch_id`, `dw_ingest_ts_utc`, `dw_extract_pattern`).
2. **Silver Layer (`silver.*`)**: Cleansed, conformed, and strongly-typed entity tables. Performs PII masking on emails/phones, maps payment method codes, and routes invalid records missing primary keys to quarantine tables (`slv_customer_quarantine`, `slv_order_quarantine`).
3. **Gold Layer (`gold.*`)**: Dimensional Star Schema optimized for business analytics. Features Slowly Changing Dimensions Type 2 (SCD2) for customer and restaurant entities, fact tables with surrogate key resolution (`-1` fallback for unknown keys), pre-seeded date/time dimensions, and aggregated business reporting marts (`mart_daily_business_summary`).

```
+------------------+      +-------------------+      +-------------------+      +------------------+
|   Source OLTP    | ---> |   Bronze Layer    | ---> |   Silver Layer    | ---> |    Gold Layer    |
| (PostgreSQL DB)  |      |   (Raw Landing)   |      |  (Conformed/PII)  |      |  (Star Schema)   |
+------------------+      +-------------------+      +-------------------+      +------------------+
                                                               |                         |
                                                               v                         v
                                                     +-------------------+      +------------------+
                                                     | Data Quality Gate |      |  Reconciliation  |
                                                     |   (silver.dq_rule)|      |  Suite (RC1-RC10)|
                                                     +-------------------+      +------------------+
```

---

## 🎧 Voice-Based Audio Guides

This repository includes custom voice-narrated `.mp3` audio walkthroughs in the `audio/` directory:

- 🎵 **[01_repository_overview.mp3](file:///config/Desktop/Session1/buildwithgemini-zwiggy-medallion-dwh-advanced/audio/01_repository_overview.mp3)**: Comprehensive walkthrough of the codebase, project structure, CLI commands, and setup instructions.
- 🎵 **[02_medallion_dwh_concepts.mp3](file:///config/Desktop/Session1/buildwithgemini-zwiggy-medallion-dwh-advanced/audio/02_medallion_dwh_concepts.mp3)**: Deep-dive explanation of Medallion architecture (Bronze/Silver/Gold), SCD2, PII masking, DQ gates, and reconciliation checks.
- 🎵 **[03_hands_on_execution_tutorial.mp3](file:///config/Desktop/Session1/buildwithgemini-zwiggy-medallion-dwh-advanced/audio/03_hands_on_execution_tutorial.mp3)**: Step-by-step hands-on tutorial on initializing the warehouse, running incremental/full batches, evaluating DQ scorecards, and inspecting watermarks.

---

## 🛠️ Quick-Start Instructions

### 1. Prerequisites
- Python 3.10+
- PostgreSQL 14+ database instances (Source OLTP & Target DWH)

### 2. Environment Setup & Installation
```bash
# Clone the repository
git clone git@github.com:yogimaindale-pixel/buildwithgemini-zwiggy-medallion-dwh-advanced.git
cd buildwithgemini-zwiggy-medallion-dwh-advanced

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r dwh/requirements.txt
```

### 3. Environment Configuration
Create a `.env` file in the root directory:
```env
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=zwiggy_db
PG_USER=postgres
PG_PASSWORD=postgres

WAREHOUSE_HOST=localhost
WAREHOUSE_PORT=5432
WAREHOUSE_DATABASE=zwiggy_db
WAREHOUSE_USER=postgres
WAREHOUSE_PASSWORD=postgres

BUSINESS_TIMEZONE=UTC
BATCH_SIZE=10000
MAX_RETRIES=3
RETRY_BACKOFF_SECONDS=2.0
```

---

## 💻 Execution Commands

### 1. Initialize Warehouse Schemas & Tables
Executes DDL scripts to create `ctl`, `bronze`, `silver`, `gold` schemas, control tables, reference data, and seed static date/time dimensions:
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli init
```

### 2. Run Pipeline (Incremental Ingestion)
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli run
```

### 3. Force Full Reload Pipeline Execution
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli run --full
```

### 4. Check Pipeline Execution Status & Watermarks
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli status
```

### 5. View Data Quality Scorecard
```bash
PYTHONPATH=dwh/src python3 -m zwiggy_dwh.cli scorecard
```

### 6. Run Automated Test Suite
```bash
PYTHONPATH=dwh/src pytest dwh/tests/
```

---

## 📂 Repository Structure

```
.
├── audio/                                   # Voice-based audio walkthrough MP3s
│   ├── 01_repository_overview.mp3
│   ├── 02_medallion_dwh_concepts.mp3
│   └── 03_hands_on_execution_tutorial.mp3
├── dwh/
│   ├── requirements.txt                    # Python dependencies
│   ├── src/
│   │   └── zwiggy_dwh/                      # Main Python package
│   │       ├── batch.py                     # Batch control, locking, & watermark management
│   │       ├── bronze.py                    # Bronze raw landing & extraction
│   │       ├── cli.py                       # Command-line interface parser & entrypoint
│   │       ├── config.py                    # Environment settings & secret masking
│   │       ├── db.py                        # Database connection pool & retry context handlers
│   │       ├── dq.py                        # Data quality rule evaluation engine
│   │       ├── gold.py                      # Gold SCD2 dimensions, facts, & reporting marts
│   │       ├── init_db.py                   # Multi-file SQL initialization runner & calendar seed
│   │       ├── metadata.py                  # Schema introspection & drift detection
│   │       ├── pipeline.py                  # 12-step master pipeline orchestrator
│   │       ├── reconcile.py                 # Reconciliation suite (RC-1 through RC-10)
│   │       ├── silver.py                    # Silver entity transformation & PII masking
│   │       └── sql/                         # SQL DDL & DML scripts
│   │           ├── ctl/                     # Control schema tables & table configurations
│   │           ├── silver/                  # Silver DDL, reference lookup maps, & DQ rules
│   │           └── gold/                    # Gold star schema DDL & semantic views
│   └── tests/                               # Pytest test suite (10/10 test modules)
├── DOCUMENTATION.md                         # Technical architecture & schema specifications
├── KNOWLEDGE_TRANSFER.md                    # Beginner-friendly KT guide & step-by-step tutorials
└── README.md                                # Quick-start guide & repository summary
```

---

## 🧪 Verification & Test Results

The test suite covers database helper routines, configuration validation, mock pipeline steps, and reconciliation logic:
```
10 passed in 0.15s
```

All source code files contain line-by-line junior developer annotations explaining function arguments, SQL queries, context managers, and business logic.

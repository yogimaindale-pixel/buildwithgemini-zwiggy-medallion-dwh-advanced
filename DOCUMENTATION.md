# 📐 Technical Architecture & Specification Document (`DOCUMENTATION.md`)

## 1. System Requirements & Architecture Specification

### 1.1 Technology Stack
- **Database Engine**: PostgreSQL 14+ (supports JSONB, PL/pgSQL functions, trigram indexes, window functions).
- **Runtime Environment**: Python 3.10+ with `psycopg2-binary`, `pydantic-settings`, `python-dotenv`, and `gTTS`.
- **Testing Framework**: `pytest` for unit and integration testing.
- **Data Architecture**: 4-Schema Medallion Data Warehouse Architecture (`ctl`, `bronze`, `silver`, `gold`).

### 1.2 Multi-Schema Layering Architecture
1. **Control Schema (`ctl`)**: Manages operational metadata, batch state locks (`ctl_batch`), pipeline execution step logs (`ctl_step_log`), source table configurations (`ctl_table_config`), extraction manifests (`ctl_extract_manifest`), incremental watermarks (`ctl_watermark`), Data Quality evaluation logs (`ctl_dq_result`), and reconciliation audit records (`ctl_reconciliation`).
2. **Bronze Schema (`bronze`)**: Landed raw data lake layer. Stores raw extracted tables (`br_<source_table>`) where all columns are landed as raw `TEXT` strings. Appends audit metadata columns (`dw_batch_id`, `dw_ingest_ts_utc`, `dw_source_table`, `dw_extract_pattern`).
3. **Silver Schema (`silver`)**: Cleansed, conformed, and strongly-typed operational entity layer (`slv_customer`, `slv_address`, `slv_restaurant`, `slv_order`, `slv_payment`). Performs PII masking on emails/phones, maps payment method codes, standardizes statuses, and isolates malformed records into quarantine tables (`slv_customer_quarantine`, `slv_order_quarantine`).
4. **Gold Schema (`gold`)**: Dimensional Star Schema optimized for OLAP analytics. Implements Slowly Changing Dimensions Type 2 (SCD2) for customer and restaurant entities (`dim_customer`, `dim_restaurant`), pre-seeded date/time dimensions (`dim_date`, `dim_time`), fact tables (`fact_order`, `fact_payment`) with surrogate key resolution (`-1` fallback for unknown keys), and aggregated business reporting marts (`mart_daily_business_summary`).

---

## 2. Table Schemas & Metadata Reference

### 2.1 Control Schema (`ctl`)

#### `ctl.ctl_batch`
Tracks execution state and timing for pipeline runs.
| Column | Type | Constraints | Description |
|---|---|---|---|
| `dw_batch_id` | `BIGSERIAL` | `PRIMARY KEY` | Unique batch run identifier |
| `run_type` | `VARCHAR(20)` | `NOT NULL` | Run mode (`INCREMENTAL`, `FULL_RELOAD`, `REPLAY`) |
| `status` | `VARCHAR(20)` | `NOT NULL` | Batch status (`RUNNING`, `SUCCEEDED`, `FAILED`, `PUBLISH_BLOCKED`) |
| `published` | `BOOLEAN` | `DEFAULT FALSE` | Flag indicating if Gold data is cleared for downstream BI |
| `cutoff_ts_utc` | `TIMESTAMPTZ` | `NOT NULL` | Ingestion cutoff timestamp boundary |
| `business_timezone` | `VARCHAR(50)` | `NOT NULL` | Configured business timezone (e.g., 'UTC') |
| `code_version` | `VARCHAR(20)` | `NOT NULL` | Application release version |
| `start_ts_utc` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Batch execution start timestamp |
| `end_ts_utc` | `TIMESTAMPTZ` | `NULL` | Batch execution end timestamp |
| `notes` | `TEXT` | `NULL` | Diagnostic summary or failure messages |

#### `ctl.ctl_watermark`
Tracks incremental extraction positions per source table.
| Column | Type | Constraints | Description |
|---|---|---|---|
| `source_table` | `VARCHAR(100)` | `PRIMARY KEY` | Operational source table name |
| `watermark_value` | `VARCHAR(255)` | `NOT NULL` | Last extracted timestamp or ID |
| `dw_batch_id` | `BIGINT` | `NOT NULL` | Batch ID that updated the watermark |
| `updated_ts_utc` | `TIMESTAMPTZ` | `DEFAULT NOW()` | Last update timestamp |

---

### 2.2 Silver Schema (`silver`)

#### `silver.slv_customer`
Conformed customer entity table with PII masking.
| Column | Type | Constraints | Description |
|---|---|---|---|
| `customer_id` | `BIGINT` | `PRIMARY KEY` | Source natural customer key |
| `name` | `VARCHAR(255)` | `NOT NULL` | Customer full name |
| `email_masked` | `VARCHAR(255)` | `NOT NULL` | SHA-256 masked email address |
| `phone_masked` | `VARCHAR(50)` | `NOT NULL` | Masked contact phone number |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Account creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Account modification timestamp |
| `dw_batch_id` | `BIGINT` | `NOT NULL` | Ingestion batch ID |

#### `silver.slv_order`
Conformed order entity table with cohort logic.
| Column | Type | Constraints | Description |
|---|---|---|---|
| `order_id` | `BIGINT` | `PRIMARY KEY` | Source natural order key |
| `customer_id` | `BIGINT` | `NOT NULL` | Customer reference key |
| `restaurant_id` | `BIGINT` | `NOT NULL` | Restaurant reference key |
| `order_status` | `VARCHAR(50)` | `NOT NULL` | Uppercase conformed status (`PLACED`, `DELIVERED`, `CANCELLED`) |
| `total_amount` | `NUMERIC(12,2)` | `NOT NULL` | Total monetary order value |
| `discount_amount` | `NUMERIC(12,2)` | `NOT NULL` | Discount amount applied |
| `delivery_fee` | `NUMERIC(12,2)` | `NOT NULL` | Delivery charge amount |
| `cohort_id` | `VARCHAR(50)` | `NOT NULL` | Derived user cohort (`COHORT_A` if ID <= 50000 else `COHORT_B`) |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Order placement timestamp |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Order modification timestamp |
| `dw_batch_id` | `BIGINT` | `NOT NULL` | Ingestion batch ID |

---

### 2.3 Gold Schema (`gold`)

#### `gold.dim_customer` (SCD Type 2)
Slowly Changing Dimension Type 2 for customers.
| Column | Type | Constraints | Description |
|---|---|---|---|
| `customer_sk` | `BIGSERIAL` | `PRIMARY KEY` | Integer Surrogate Key |
| `customer_id` | `BIGINT` | `NOT NULL` | Natural customer key |
| `name` | `VARCHAR(255)` | `NOT NULL` | Customer name |
| `email_masked` | `VARCHAR(255)` | `NOT NULL` | Masked email address |
| `phone_masked` | `VARCHAR(50)` | `NOT NULL` | Masked phone number |
| `valid_from` | `TIMESTAMPTZ` | `NOT NULL` | SCD2 validity start timestamp |
| `valid_to` | `TIMESTAMPTZ` | `NULL` | SCD2 validity end timestamp (NULL if current) |
| `dw_is_current` | `BOOLEAN` | `DEFAULT TRUE` | True if latest active record |
| `dw_version` | `INT` | `DEFAULT 1` | Incremental record version number |
| `dw_batch_id` | `BIGINT` | `NOT NULL` | Batch ID that created the record |

#### `gold.fact_order`
Fact table for platform order transactions.
| Column | Type | Constraints | Description |
|---|---|---|---|
| `order_sk` | `BIGSERIAL` | `PRIMARY KEY` | Fact Surrogate Key |
| `order_id` | `BIGINT` | `NOT NULL` | Natural order ID |
| `customer_sk` | `BIGINT` | `NOT NULL` | Foreign Key to `gold.dim_customer` (`-1` fallback) |
| `restaurant_sk` | `BIGINT` | `NOT NULL` | Foreign Key to `gold.dim_restaurant` (`-1` fallback) |
| `order_date_sk` | `INT` | `NOT NULL` | Date key (YYYYMMDD format) |
| `order_time_sk` | `INT` | `NOT NULL` | Time key (HHMM format) |
| `order_status` | `VARCHAR(50)` | `NOT NULL` | Conformed order status |
| `total_amount` | `NUMERIC(12,2)` | `NOT NULL` | Total order amount |
| `discount_amount` | `NUMERIC(12,2)` | `NOT NULL` | Discount amount |
| `delivery_fee` | `NUMERIC(12,2)` | `NOT NULL` | Delivery fee |
| `cohort_id` | `VARCHAR(50)` | `NOT NULL` | User cohort ID |
| `dw_batch_id` | `BIGINT` | `NOT NULL` | Ingestion batch ID |

---

## 3. Core Business Rules & Quality Rules

### 3.1 Business Transformations
1. **PII Masking**: Customer email and phone number columns are passed through `silver.mask_pii()` to preserve privacy while supporting join hashing.
2. **Cohort Assignment**: Orders are automatically tagged into `COHORT_A` (if `order_id <= 50000`) or `COHORT_B` (if `order_id > 50000`).
3. **Payment Method Standardization**: Raw payment method strings (e.g., 'cc', 'upi') are normalized using `silver.ref_payment_method_map`.

### 3.2 Data Quality Rules (`silver.dq_rule`)
- `DQ-CUST-01`: Customer ID must not be null or empty string. (Severity: `BLOCK`)
- `DQ-ORD-01`: Order total amount must be non-negative (`total_amount >= 0`). (Severity: `BLOCK`)
- `DQ-ORD-02`: Order created timestamp must not be in the future. (Severity: `WARN`)
- `DQ-PAY-01`: Payment amount must be non-negative (`amount >= 0`). (Severity: `BLOCK`)

### 3.3 Reconciliation Suite Checks (RC-1 to RC-10)
- `RC-1`: Extracted source rows equal Bronze landed rows.
- `RC-2`: Bronze landed rows accounted for in Silver (Loaded + Quarantined).
- `RC-3`: 1:1 row count matching between Silver order and Gold fact_order tables.
- `RC-4`: Total order amount dollar sum consistency between Silver and Fact layers (`$0.00` variance).
- `RC-5`: Order financial sum cross-checked against completed payment sums.
- `RC-6`: Unknown Surrogate Key (`customer_sk = -1`) share must not exceed 5%.
- `RC-7`: Zero duplicate order IDs in fact_order for batch.
- `RC-8`: SCD2 valid date ranges in `dim_customer` must not overlap.
- `RC-9`: Mart daily summary total order count matches fact_order total count.
- `RC-10`: Gold fact tables must contain fresh ingestion timestamps.

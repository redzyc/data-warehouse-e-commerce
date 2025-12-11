# E-Commerce Data Warehouse Pipeline

An end-to-end **data engineering pipeline** for e-commerce analytics, built with **Docker**, **Apache Spark**, **Hive**, and **Jenkins** for automated orchestration.

**Status:** Red, Green & Yellow Phases Completed (Infrastructure, Ingestion & Gold Layer)

---

## Table of Contents
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Data Warehouse Schema](#data-warehouse-schema)
- [Key Concepts](#key-concepts)
- [Development](#development)

---

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Containerization | Docker & Docker Compose | Latest |
| Distributed Storage | Apache Hadoop HDFS | 3.2.1 |
| Data Warehouse | Apache Hive | 2.3.2 |
| Processing Engine | Apache Spark | 3.5.0 |
| Job Orchestration | Jenkins | Latest |
| Language | Python (PySpark) | 3.x |
| Metadata Store | PostgreSQL | 9.6+ |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Jenkins Orchestrator                     │
│            (Scheduled Jobs & Pipeline Automation)            │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   ┌─────────┐   ┌──────────┐   ┌──────────┐
   │ Generators│  │Generators│  │ Spark    │
   │ (Products,│  │(Logs, TX)│  │ Jobs     │
   │Countries) │  │(PyScript)│  │(PySpark) │
   └─────────┬┘   └────┬─────┘   └────┬─────┘
             │         │             │
             └─────────┼─────────────┘
                       ▼
          ┌──────────────────────────┐
          │   HDFS (Data Lake)       │
          │  /user/root/ecommerce/   │
          └────────────┬─────────────┘
                       ▼
        ┌──────────────────────────────┐
        │  Hive Metastore (PostgreSQL) │
        │   Metadata & Governance      │
        └────────────┬─────────────────┘
                     ▼
        ┌────────────────────────────┐
        │  Hive Warehouse Database   │
        │   ecommerce_db Tables      │
        └────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- **Docker Desktop** installed and running
- Minimum **6GB RAM** allocated to Docker
- **~5-10 minutes** initialization time on first start

### Step 1: Start the Cluster

```bash
cd docker/
docker-compose up -d --build
```

**Expected output:**
- 10+ containers starting (Hadoop, Hive, Spark, Jenkins, etc.)
- Check status: `docker-compose ps`

### Step 2: Initialize Infrastructure

1. Open **Jenkins** at [http://localhost:9090](http://localhost:9090)
   - Initial Admin Password: Check Jenkins logs or use default
   - `docker logs jenkins | grep -i "initial admin"`

2. Run **Setup_Infrastructure** job
   - This initializes HDFS directories, Hive database schema, and creates tables
   - Logs available in Jenkins UI

### Step 3: Access Web UIs

| Service | URL | Purpose |
|---------|-----|---------|
| **Jenkins** | [http://localhost:9090](http://localhost:9090) | Job orchestration & scheduling |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | Spark cluster monitoring |
| **HDFS NameNode** | [http://localhost:9870](http://localhost:9870) | Storage administration |
| **ResourceManager** | [http://localhost:8088](http://localhost:8088) | Hadoop YARN resource tracking |
| **Hue** | [http://localhost:8888](http://localhost:8888) | Hive query editor & database browser |
| **Superset** | [http://localhost:8089](http://localhost:8089) | BI Dashboard

---

## Project Structure

```
data-warehouse-e-commerce/
├── docker/
│   ├── docker-compose.yaml          # Full cluster definition
│   ├── Dockerfile.jenkins            # Custom Jenkins with docker-in-docker
│   ├── hadoop.env                    # Hadoop configuration
│   ├── hadoop-hive.env               # Hive configuration
│   ├── jenkins_home/                 # Jenkins persistent state
│   │   └── jobs/
│   │       ├── Setup_Infrastructure/ # Initialize system
│   │       ├── Daily_Data_Pipeline/  # Master data refresh
│   │       └── Hourly_Data_Pipeline/ # Transaction ingestion
│   └── conf.dist/                    # Hue & logging configs
│
├── src/
│   ├── processing/                   # Spark ETL Jobs
│   │   ├── ingest_logs.py            # Ingest raw transaction logs
│   │   ├── ingest_products.py        # Ingest product master data (SCD Type 2)
│   │   ├── ingest_countries.py       # Ingest country master data (SCD Type 0)
│   │   └── metastore_db/             # Internal Spark Derby metastore
│   │
│   └── script/
│       ├── init_hive.sql             # Hive schema creation
│       ├── data_generator.py          # Transaction log generator
│       ├── product_and_countries_generator.py  # Master data generator
│       ├── generator/                 # Generator utilities
│       └── ingestion/                 # Ingestion utilities
│
├── data/                              # Local data staging (CSV before HDFS)
├── README.md                          # This file
└── LICENSE
```

---

## Data Flow & Pipelines

### Data Pipeline Schedule

| Job | Frequency | Triggers | Output |
|-----|-----------|----------|--------|
| **Setup_Infrastructure** | On-demand | Manual trigger via Jenkins | Initialize database & HDFS |
| **Daily_Data_Pipeline** | Daily | Cron schedule | Generates and ingests countries and products |
| **Hourly_Data_Pipeline** | Every hour | Cron schedule | Generates and ingests raw logs |
| **Hourly_Data_Pipeline** | Every 2 hours | Cron schedule | Generates and ingests raw logs (Increased frequency) |
| **Maintenance_Recovery** | Daily (7 AM) | Post-Daily Pipeline | Retries failed transactions after master data updates |
| **System_Backup_Gold** | Daily (2 AM) | Cron schedule | Dumps and archives Postgres Gold database |

### Data Ingestion Flow

#### 1. **Transaction Logs (Hourly)**
```
Generator → CSV Files (HDFS) → Spark Ingestion Job → transactions_raw (Parquet)
```
- **Schema:** `invoice_no, stock_code, quantity, invoice_date, customer_id, country_id, region_id, ingestion_timestamp`
- **Validation:** Validates Foreign Keys (Products & Countries exist in warehouse)
- **Mode:** APPEND (append-only fact table)

#### 2. **Product Master Data (Daily)**
```
Generator → CSV Files (HDFS) → Spark Ingestion Job → products (Parquet)
```
- **Schema:** `stock_code, description, unit_price, price_date`
- **Type:** Slowly Changing Dimension Type 2 (keeps history)
- **Mode:** OVERWRITE with lineage break technique (collect → repartition → overwrite)

#### 3. **Country Master Data (Daily)**
```
Generator → CSV Files (HDFS) → Spark Ingestion Job → countries (Parquet)
```
- **Schema:** `country_id, country_name, continent`
- **Type:** Slowly Changing Dimension Type 0 (no history)
- **Mode:** OVERWRITE

---

## Data Warehouse Schema

The project implements a classic **Kimball Star Schema** optimized for BI reporting in Hive.

### Schema: ecommerce_dw

| Table Type | Table Name | Description | Key Strategy |
|:---|:---|:---|:---|
| Fact | `fact_transactions` | Central transactional metrics (Sales, Quantity). | Partitioned by Year/Month. Links to dimensions via Foreign Keys. |
| Dimension | `dim_products` | Product details and price history. | SCD Type 2 using Surrogate Key (`product_sk`) to track historical price changes. |
| Dimension | `dim_countries` | Geo-location data. | SCD Type 0 (Overwrite). Includes `region_code` for regional analytics. |
| Dimension | `dim_customers` | User profiles. | SCD Type 0. Tracks unique customers based on ID. |

Note: The static time dimension table `dim_datetime` has been removed from the persistent Hive schema. Time attributes (year, quarter, month, day, day_of_week, is_weekend, etc.) are computed on-the-fly during Spark transformations and are persisted in the Gold/Postgres layer as needed.

---

## Data Warehouse Schema (Gold Layer)

The project implements a Star Schema in PostgreSQL (database: `ecommerce_gold`). The Gold layer holds the production-ready, denormalized schema optimized for BI consumption and reporting.

Schema: `ecommerce_gold`

| Table Type | Table Name | Description | Key Strategy |
|:---|:---|:---|:---|
| Fact | `fact_transactions` | Central transactional metrics and measures. | Partition by date at ETL level; use primary key (hashed) and upserts to ensure idempotency. |
| Dimension | `dim_products` | Product master with price history. | Up-to-date attributes; include `effective_start_date` for history. |
| Dimension | `dim_countries` | Geographic master. | Overwrite-on-update (Type 0); include `region_code` for regional analytics. |
| Dimension | `dim_customers` | Customer master. | Overwrite-on-update (Type 0); de-duplicated by customer identifier. |

Example table definitions (concise):

Fact table `fact_transactions` (recommended columns):

```
transaction_id TEXT PRIMARY KEY, -- generated hash of key fields
invoice_no TEXT,
product_id TEXT,
country_id TEXT,
quantity INT,
unit_price NUMERIC(10,2),
total_amount NUMERIC(12,2),
invoice_date TIMESTAMP,
ingestion_timestamp TIMESTAMP
```

Dimension `dim_products` (recommended columns):

```
product_id TEXT PRIMARY KEY,
stock_code TEXT,
description TEXT,
unit_price NUMERIC(10,2),
effective_start_date TIMESTAMP
```

Dimension `dim_countries` (recommended columns):

```
country_id TEXT PRIMARY KEY,
country_name TEXT,
region_code TEXT,
continent TEXT
```

Error handling table:

`transactions_error` — stores rows that fail business validation (missing FK, malformed payload) with columns such as `error_id`, `payload JSON`, `error_message`, `source_file`, `attempts`, and `ingestion_timestamp`. This table enables retry and manual investigation workflows.

Key Concepts & Patterns for Gold Layer

---

## Green Phase: Architecture Evolution (Direct Bronze-to-Gold Loading)

Decision: Skipping Persistent Silver Layer

Context: The initial plan included a persisted Silver layer in Hive between Bronze and Gold.

Decision: We adopted a Direct Load architecture: Extract from Hive Bronze -> Transform in Spark memory -> Load to Postgres Gold.

Rationale:
- Simplification: For the observed data volume, an intermediate persistent Hive Silver layer added storage overhead and latency without measurable benefit.
- Speed: Direct loading updates the BI Postgres database immediately after transformation, reducing end-to-end latency for dashboards.
- Efficiency: Spark performs complex "as-of" joins and deduplication in memory, avoiding unnecessary HDFS writes.

Spark & Postgres Integration

Challenge: `ClassNotFoundException: org.postgresql.Driver`

Problem: Spark jobs initially failed to load the PostgreSQL JDBC driver when it was configured inside Python.

Root Cause: The JVM classpath is established when `spark-submit` launches; setting `spark.jars` from within Python is too late for the driver to be available to the JVM.

Solution: Move JDBC driver configuration into the shell wrapper that launches `spark-submit` so the driver is available to the JVM at startup. Example wrapper:

```bash
/opt/spark/bin/spark-submit \
   --jars /opt/spark/jars-custom/postgresql-42.7.8.jar \
   --conf spark.driver.extraClassPath=/opt/spark/jars-custom/postgresql-42.7.8.jar \
   --conf spark.executor.extraClassPath=/opt/spark/jars-custom/postgresql-42.7.8.jar \
   /opt/spark/jobs/postgres_load_fact.py
```

Postgres Drops Blocked by Active Sessions

Problem: The setup pipeline could not `DROP DATABASE ecommerce_gold` because active client sessions existed.

Solution: Terminate active sessions before drop using `pg_terminate_backend`, then DROP/CREATE as the superuser:

```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'ecommerce_gold' AND pid <> pg_backend_pid();
-- then DROP DATABASE ecommerce_gold; CREATE DATABASE ecommerce_gold;
```

Complex Transformation Logic

Historical Price Matching (As-Of Join)
- Problem: Transactions do not carry unit_price; prices change over time and must be matched to the price in effect at invoice_date.
- Solution: Use a windowed approach:
   - Join transactions to product price history on `stock_code`.
   - Filter where `price_date <= invoice_date`.
   - Compute `row_number() OVER (PARTITION BY transaction_id ORDER BY price_date DESC)` and keep `rn = 1`.

Product Deduplication / Latest Snapshot
- Build a latest snapshot for `dim_products` using a window partitioned by `stock_code` ordered by `price_date DESC`, filter `rn = 1`, then write to Postgres using overwrite or upsert.

Hive Self-Read/Write Deadlock
- Problem: Overwriting a Hive table while reading from it in the same job triggered `[UNSUPPORTED_OVERWRITE]`.
- Solution: Use a temporary view to isolate the read set and perform an atomic table swap (create/replace) or write to a temp table then rename.

Data Quality & Error Handling

Missing Reference Data (Foreign Keys)
- Implement a Split-Stream pattern:
   - Left-join transactions with `dim_countries`.
   - Valid rows are written to `fact_transactions` in Postgres.
   - Invalid rows (null country_id) are written to `transactions_error` in Postgres for review and retry.

Green Phase Status

Completed Successfully

- Gold Layer established (`ecommerce_gold` database in Postgres)
- Robust ETL: Python transformation logic separated from shell-level infrastructure config
- Transformation Logic: As-of joins and deduplication implemented in Spark
- Data Quality: Invalid rows captured in `transactions_error` rather than discarded
- Automation: Jenkins jobs updated to run Direct-to-Gold pipelines

System Ready for Purple Phase (Anomaly Detection & Alerting)

---

1. Direct Load Architecture (ELT)
- Extract from Hive (Bronze) → Transform in Spark memory → Load to Postgres (Gold). This reduces latency between ingestion and BI availability and simplifies the pipeline by avoiding an intermediate persisted Silver layer.

2. JDBC Driver Management
- Spark jobs include the PostgreSQL JDBC driver dynamically (via `--jars` and classpath entries) in the shell wrapper scripts under `/opt/spark/scripts/postgres/` so jobs can connect to Postgres during execution.

3. Idempotency and Upserts
- Gold jobs are idempotent. For the fact table we recommend using `INSERT ... ON CONFLICT (transaction_id) DO UPDATE ...` (Postgres upsert semantics) or load-to-staging then swap. Dimension loads can use `TRUNCATE`/`OVERWRITE` or `MERGE` depending on size and SLA.

Development: Running Gold ETL Manually

Run the Fact ETL and load to Postgres from the Spark master container:

```bash
docker exec -it spark-master /opt/spark/scripts/postgres/run_fact_postgres.sh
```

Check data in Postgres (example using `psql` inside the Postgres container or via your SQL client):

```bash
# using psql inside container (replace user/db as appropriate)
docker exec -it postgres psql -U postgres -d ecommerce_gold -c "SELECT * FROM fact_transactions LIMIT 10;"

# check error table
docker exec -it postgres psql -U postgres -d ecommerce_gold -c "SELECT * FROM transactions_error LIMIT 10;"
```

Notes
- Ensure the Postgres credentials and network are configured in the Spark job wrapper scripts and Jenkins job environment variables.
- Monitor JDBC batch sizes and commit frequency to control memory and transaction overhead.

---

---

## Key Concepts & Patterns

### Data Storage
- **HDFS Path Structure:** `/user/root/ecommerce/{logs,products,countries}/`
- **Hive Database:** `ecommerce_db`
- **Format:** Parquet (columnar, compressed)
- **Warehouse Dir:** `/user/hive/warehouse`

### Data Integrity Features
1. **Idempotent Jobs:** All ingestion jobs can be safely re-run without duplicating data
2. **Foreign Key Validation:** Transaction generator fetches real Product IDs from Hive before creating logs
3. **Timestamp Tracking:** Each record includes `ingestion_timestamp` for data lineage
4. **Self-Healing:** Spark configured with extended timeouts and retry logic for zombie process handling

### SCD Implementation
- **Type 0 (countries):** Overwrite completely (latest values only)
- **Type 2 (products):** Keep full history (lineage break + collect-to-driver technique)

### Critical Configuration
- **Spark Metastore:** Connected to Hive via `thrift://hive-metastore:9083`
- **Spark Worker:** Limited to 1 core, 512MB memory (development setup)
- **Timeouts:** `spark.network.timeout=120s`, `spark.rpc.askTimeout=60s` (for zombie handling)

---

## Development

### Running Jobs Locally

**Spark SQL in Container:**
```bash
docker exec -it spark-master spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083
```

**Viewing HDFS:**
```bash
docker exec -it namenode hdfs dfs -ls /user/root/ecommerce/
```

**Querying Hive in Container:**
```bash
docker exec -it hive-server hive -e "SELECT * FROM ecommerce_db.transactions_raw LIMIT 10;"
```

### Common Tasks

**Check Pipeline Status:**
- Jenkins: [http://localhost:9090](http://localhost:9090) → View job build history

**Monitor Spark Jobs:**
- Spark Master: [http://localhost:8080](http://localhost:8080) → Applications

**Browse HDFS:**
- NameNode: [http://localhost:9870](http://localhost:9870) → Utilities → Browse HDFS

**Query Data (Easy UI):**
- Hue: [http://localhost:8888](http://localhost:8888) → New Query

**BI Dashboard:**
- Superset [http://localhost:8089](http://localhost:8089) 

### Troubleshooting

---

## Purple Phase: Anomaly Detection & Alerting

This phase implements a statistical monitoring system to detect anomalous orders (potential fraud or data errors) in the Gold Layer using historical sales patterns.

### Statistical Anomaly Detection

The system applies **Z-Score analysis** to identify orders that deviate significantly from normal patterns:

1. **Baseline Computation (per product)**
   - **Median Quantity:** Calculated via `percentile_approx(0.5)` on historical order quantities per `stock_code`
   - **Standard Deviation:** Measured across all quantities for each product

2. **Anomaly Trigger**
   - An order is flagged if: `Order_Quantity > Median + (2 × Std_Deviation)`
   - This threshold captures orders in the top ~2.3% of the distribution (assuming normality)

### Implementation Architecture

| Component | Technology | Purpose |
|-----------|-----------|----------|
| Detection Engine | PySpark job (`detect_anomalies.py`) | Computes Z-scores and writes flagged orders to Postgres |
| Scheduling | Jenkins | Runs immediately post-Gold ETL |
| Alerting | Python `smtplib` via Gmail SMTP | Sends HTML-formatted email notifications |
| Credential Management | `.env` file (local, not committed) | Secures SMTP credentials outside Git |
| BI Visualization | Tableau | Connects to Postgres Gold Layer for dashboards |

### Alert Output

Flagged anomalies are persisted to a dedicated Postgres table (`anomalies`) with:
- `order_id` — Unique transaction identifier
- `stock_code` — Product code
- `quantity` — Order quantity that triggered alert
- `median_qty` — Product's baseline median
- `std_deviation` — Product's standard deviation
- `z_score` — Calculated Z-score (magnitude of deviation)
- `alert_timestamp` — Detection time
- `status` — Investigation state (NEW, REVIEWED, RESOLVED)

### BI Dashboards

Tableau dashboards connected to the Postgres Gold Layer enable stakeholders to:
- **Revenue by Geography:** Visualize total sales and count by country
- **Top 20 Products:** Bar chart of best-selling items by quantity and revenue
- **Anomaly Trend Analysis:** Time-series scatter plot showing flagged orders and statistical thresholds over time
- **Anomaly Drill-Down:** Filter by product, date range, or Z-score threshold for root-cause analysis

### Data Quality & Feedback Loop

- **False Positives:** Legitimate bulk orders can be marked RESOLVED to update baseline thresholds
- **True Anomalies:** Investigated orders provide feedback for fraud prevention or data quality improvements
- **Baseline Refresh:** Periodically recompute median and standard deviation to adapt to seasonal trends

---

## Blue Phase: Production Hardening & BI Evolution

This phase focused on system stability, self-healing capabilities, schema evolution, and open-source BI integration.

### 1. Error Recovery ("The Hospital Pattern")

**Problem:** Orders arriving with `country_id`s not yet present in the master data caused data loss or pipeline failures.
**Solution:** Implemented a **Split-Stream ETL** with a "Dead Letter Queue" pattern.



- **Main ETL (`etl_direct_fact.py`):**
    - Checks incoming `country_id` against `dim_countries`.
    - **Valid Rows:** Processed normally to `fact_transactions`.
    - **Invalid Rows:** Redirected to `transactions_error` table (The Hospital) with a "Missing Country ID" flag.
- **Recovery Job (`recover_missing_countries.py`):**
    - Runs daily after the Country Master update.
    - Checks the "Hospital" table against the *new* master data.
    - **Fixed Rows:** Enriched with pricing/currency and injected into `fact_transactions`.
    - **Still Broken:** Remain in the hospital for the next day.

### 2. Robust Date Parsing & Schema Evolution

**Date Validation Strategy:**
To handle inconsistent upstream date formats (e.g., mixing `dd/MM/yyyy` and `yyyy-MM-dd`), the pipeline now uses a **Coalesce Strategy**:
1.  Attempts to parse `invoice_date` using a list of 4 supported formats.
2.  If all fail, assigns a **Sentinel Value** (`1900-01-01`) and flags the row as `is_date_error`.
3.  Ensures the pipeline never crashes due to timestamp format changes.

**Schema Evolution:**
- **ID Standardization:** All IDs (`invoice`, `stock`, `customer`) are explicitly cast to `STRING` to prevent type mismatches (e.g., "12345" vs "A12345").
- **Multi-Currency Support:** Added `currency` column (default 'GBP') to the Fact table to support future international expansion.

### 3. Operational Resilience (Backups & Frequency)

- **Backup Strategy:** Automated Shell script dumps the `ecommerce_gold` Postgres database every night at 02:00 AM, compresses it (`.gz`), and enforces a 7-day retention policy (deletes older backups).
- **Latency Optimization:** Increased main pipeline frequency from **Every 4 Hours** to **Every 2 Hours** to provide fresher data to business stakeholders.

### 4. BI Visualization (Apache Superset)

**Decision:** Replaced Tableau with **Apache Superset** to maintain a fully containerized, open-source stack.

- **Integration:** Superset runs as a Docker service (`superset`), connected to the `ecommerce_gold` Postgres database.
- **Metadata:** Uses the existing Hive Metastore Postgres instance to store its own configuration (dashboards, users), minimizing resource footprint.
- **Dashboard:** Implemented "E-Commerce Overview" answering key business questions:
    - *Sales per Region* (Geo Maps)
    - *Order Frequency Distribution* (Histograms)
    - *Cross-Border Shoppers* (SQL Lab Custom Queries)

---


## Project Phases

| Phase | Status | Scope |
|-------|--------|-------|
| **Red** | Completed | Infrastructure setup & Bronze ingestion (Hive) |
| **Green** | Completed | Gold Layer (Postgres), Transformations & Error Handling |
| **Yellow** | Completed | Build dimensional data warehouse |
| **Purple** | Completed | Anomaly detection, alerting, and BI visualization |
| **Blue** | Completed | Advanced BI dashboards & operational monitoring |

---

## Additional Resources

- **Spark Documentation:** [Apache Spark 3.5.0](https://spark.apache.org/docs/3.5.0/)
- **Hive Documentation:** [Apache Hive 2.3.2](https://cwiki.apache.org/confluence/display/Hive)
- **Docker Reference:** [Docker Compose](https://docs.docker.com/compose/)

---

**Repository:** `data-warehouse-e-commerce` | **Branch:** `feature/red-phase`

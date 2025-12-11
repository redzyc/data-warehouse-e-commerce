# E-Commerce Data Warehouse Engineering Diary

## Project Goal

Build an end-to-end data pipeline for an e-commerce platform serving two primary stakeholders:

1. **Analytics Team** — BI dashboard for business intelligence and trend analysis
2. **End-Users** — Purchase history and transaction tracking views

---

## Data Sources & Ingestion Schedule

| Source | Frequency | SLA | Purpose |
|--------|-----------|-----|---------|
| **Transaction Logs** | Every 4 hours | Real-time transactional data | `transactions_raw` table |
| **Products Master** | Daily (6 AM) | Updated pricing & inventory | `products` table (SCD Type 2) |
| **Countries Master** | Daily (6 AM) | Geolocation & regional data | `countries` table (SCD Type 0) |
| **System Ready** | Daily | **10:00 AM** | All data mart views must be available |

---

## Project Phases

| Phase | Status | Objectives |
|-------|--------|-----------|
| **Red** | Completed | Infrastructure setup, core ingestion jobs, Docker orchestration |
| **Green** | Completed | Gold Layer (Postgres), Transformations & Error Handling |
| **Yellow** | Completed | Dimensional warehouse modeling (star schema) |
| **Purple** | Completed | Anomaly detection, alerting, and BI visualization |
| **Blue** | Planned | Advanced BI dashboards & operational monitoring |

---

# Engineering Diary: Red Phase

## Infrastructure & Docker

### Challenge: Hive Metastore Persistence
**Problem:** The Hive database (`ecommerce_db`) was deleted on every Docker restart.

**Root Cause:** Hive metastore data was stored in container ephemeral storage instead of a persistent volume.

**Solution:** Added a named volume `hive_metastore_db` to the PostgreSQL metastore container in `docker-compose.yaml`:
```yaml
hive-metastore-postgresql:
  volumes:
    - hive_metastore_db:/var/lib/postgresql/data
```

---

### Challenge: Spark Jobs Hanging
**Problem:** Spark jobs hung indefinitely with error: `Initial job has not accepted resources for over 120 seconds`.

**Root Cause:** `spark.driver.host` was set to an internal IP address that workers couldn't resolve reliably.

**Solution:** Configured Spark to use container hostname instead of IP:
```env
SPARK_CONF_spark_driver_host=spark-master
```

---

### Challenge: Jenkins Docker-in-Docker
**Problem:** Jenkins couldn't execute `docker exec` commands to trigger Spark jobs.

**Root Cause:** Standard Jenkins image lacks Docker CLI tools.

**Solution:** Built a custom Jenkins Docker image with docker-cli installed:
```dockerfile
FROM jenkins/jenkins:latest
RUN apt-get update && apt-get install -y docker.io
```

---

## Spark & Hive Data Processing

### Challenge: Hive Metastore Connection
**Problem:** Spark SQL CLI ignored the Hive metastore and used local Apache Derby instead, causing jobs to work in Spark but not persist to Hive.

**Root Cause:** Spark containers weren't configured with Hive metastore connection details.

**Solution:** Mounted `hive-site.xml` with Thrift URI into Spark containers:
```xml
<property>
  <name>hive.metastore.uris</name>
  <value>thrift://hive-metastore:9083</value>
</property>
```

And configured Spark session:
```python
.config("hive.metastore.uris", "thrift://hive-metastore:9083")
.enableHiveSupport()
```

---

### Challenge: HDFS Location Conflicts
**Problem:** Setup job failed with `Location Already Exists` error even after dropping and recreating the database.

**Root Cause:** Old HDFS paths persisted even after Hive table deletion.

**Solution:** Added hard cleanup before table creation in `init_hive.sql`:
```bash
hdfs dfs -rm -r /user/hive/warehouse/ecommerce_db.db
```

---

### Challenge: SCD Type 0 Overwrite Error
**Problem:** `[UNSUPPORTED_OVERWRITE]` error when attempting to update the Countries table (Slowly Changing Dimension Type 0).

**Root Cause:** Spark couldn't directly overwrite Parquet files with incompatible schema operations.

**Solution:** Implemented "Lineage Break" technique:
1. Collect DataFrame to driver memory
2. Create a fresh repartitioned DataFrame
3. Write with `mode("overwrite")`

```python
df_final = df_processed.repartition(1)
df_final.write.mode("overwrite").insertInto("ecommerce_db.countries")
```

---

### Challenge: Invalid Foreign Keys in Generators
**Problem:** Transaction generator created transaction records with Product IDs that didn't exist in the warehouse, causing join failures downstream.

**Root Cause:** Generator used synthetic IDs without validation against Hive.

**Solution:** Updated generator to fetch real Product IDs directly from Hive before creating logs:
```python
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
valid_products = spark.sql("SELECT stock_code FROM ecommerce_db.products").collect()
product_ids = [row.stock_code for row in valid_products]
# Use only valid_products for transaction generation
```

---

## Red Phase Status

**Completed Successfully**

- Docker infrastructure stable with persistent storage
- Spark cluster properly configured and operational
- Hive metastore connected and queryable
- Jenkins orchestration automated with scheduled jobs
- Data integrity constraints enforced (FK validation)
- Three core ingestion pipelines operational (logs, products, countries)
- SCD Type 0 & Type 2 implementations working correctly

**System Ready for Green Phase** (Data Marts & Aggregations)
 
---

# Engineering Diary: Green Phase

## Decision: Direct Bronze-to-Gold Architecture (Skipping Silver Layer)

**Requirement:** Minimize latency between data ingestion and BI availability while simplifying the ETL pipeline.

**Context:** The initial design included a persistent Silver layer (Hive tables) as an intermediate transformation stage before loading Gold.

**Decision:** Adopted a **Direct Load architecture**: Extract from Hive Bronze → Transform in Spark memory → Load to Postgres Gold.

**Rationale:**
- **Simplification:** For this data volume, maintaining a persistent Hive Silver layer added unnecessary HDFS storage overhead without proportional analytical benefit.
- **Speed:** Direct loading eliminates intermediate write/read cycles; the BI database updates within seconds of transformation completion.
- **Efficiency:** Spark executes complex transformations (as-of joins, deduplication) in-memory at native speed, avoiding redundant disk I/O.
- **Cost:** Reduces storage footprint and cluster resource contention.

---

## Challenge: JDBC Driver ClassNotFoundException

**Problem:** Spark jobs failed with `java.lang.ClassNotFoundException: org.postgresql.Driver` despite configuring the driver path in Python.

**Root Cause:** The JVM classpath is locked when `spark-submit` starts. Configuring `spark.jars` via `SparkSession.builder()` happens after JVM initialization, which is too late for the JDBC driver to be available.

**Solution:** Moved JDBC driver configuration into the shell script wrapper that invokes `spark-submit`, ensuring the driver is available to the JVM at startup:

```bash
#!/bin/bash
/opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-custom/postgresql-42.7.8.jar \
  --conf spark.driver.extraClassPath=/opt/spark/jars-custom/postgresql-42.7.8.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars-custom/postgresql-42.7.8.jar \
  /opt/spark/jobs/postgres_load_fact.py
```

---

## Challenge: Postgres Database Drops Blocked by Active Sessions

**Problem:** The setup pipeline could not `DROP DATABASE ecommerce_gold` during re-initialization; error: "database is being accessed by other users".

**Root Cause:** Postgres prevents dropping a database while active connections exist (e.g., from abandoned client sessions or hanging jobs).

**Solution:** Implemented forceful session termination in the setup job using `pg_terminate_backend`:

```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'ecommerce_gold' AND pid <> pg_backend_pid();

-- Now safe to drop and recreate
DROP DATABASE IF EXISTS ecommerce_gold;
CREATE DATABASE ecommerce_gold;
```

---

## Challenge: Historical Price Matching (As-Of Join)

**Problem:** Transaction logs contain quantity but not unit_price. Prices change over time, and a naive join produces incorrect results: standard joins either duplicate rows (many-to-many) or match future prices (incorrect business logic).

**Root Cause:** We needed to match each transaction to the most recent price that was effective on or before the transaction date.

**Solution:** Implemented a window-function-based as-of join in Spark SQL:

```python
# Join transactions to product prices
joined = transactions.join(
    products,
    (transactions.stock_code == products.stock_code) & \
    (products.price_date <= transactions.invoice_date),
    "left"
)

# Rank by date descending, keep the most recent price
from pyspark.sql.functions import row_number, desc
window_spec = Window.partitionBy("transaction_id").orderBy(desc("price_date"))
result = joined.withColumn("rn", row_number().over(window_spec)) \
    .filter("rn = 1") \
    .select("transaction_id", "stock_code", "quantity", "unit_price", "invoice_date")
```

**Outcome:** Each transaction is matched to exactly one price (the one in effect at purchase time), ensuring historical accuracy.

---

## Challenge: Hive Self-Read/Write Deadlock

**Problem:** `[UNSUPPORTED_OVERWRITE]` error when attempting to read from a Hive table and overwrite it in the same Spark job.

**Root Cause:** Spark prevents overwriting a table that is actively being read to prevent data loss and guarantee consistency.

**Solution:** Materialized the read set as a temporary view and performed an atomic table swap:

```python
# Read and process
df_raw = spark.table("ecommerce_db.products")
df_processed = df_raw.filter(...).select(...)

# Register as temporary view
df_processed.createOrReplaceTempView("temp_products")

# Atomic swap using SQL
spark.sql("""
CREATE OR REPLACE TABLE ecommerce_db.products AS
SELECT * FROM temp_products
""")
```

**Outcome:** Read and write operations are logically separated, avoiding the deadlock.

---

## Challenge: Missing Reference Data (Foreign Keys)

**Problem:** Some transactions arrived with `country_id` values that did not exist in the Country Master table. Joining these invalid rows downstream caused data quality issues and left-join failures in the BI layer.

**Root Cause:** The transaction generator was not validating foreign keys against the current state of the warehouse.

**Solution:** Implemented a **Split-Stream pattern**:

1. **Validation:** Left-join transactions with the countries dimension
2. **Valid Stream:** Rows with non-null country_id are written to `fact_transactions` in Postgres (Gold)
3. **Error Stream:** Rows with null country_id are written to `transactions_error` in Postgres for manual review and remediation

```python
valid_transactions = joined_data.filter("country_id IS NOT NULL")
error_transactions = joined_data.filter("country_id IS NULL")

valid_transactions.write.mode("append") \
    .option("url", postgres_url) \
    .option("dbtable", "fact_transactions") \
    .save()

error_transactions.write.mode("append") \
    .option("url", postgres_url) \
    .option("dbtable", "transactions_error") \
    .save()
```

**Outcome:** Data quality issues are captured and tracked; operations continue without blocking on invalid data.

---

## Green Phase Status

**Completed Successfully**

- Gold Layer established: `ecommerce_gold` database in PostgreSQL
- Architecture: Direct Bronze-to-Gold ELT with in-memory transformations
- JDBC Integration: Driver properly injected at `spark-submit` time
- Transformation Logic: As-of joins and deduplication implemented and tested
- Data Quality: Invalid rows captured in error tables for triage and retry
- Automation: Jenkins jobs orchestrate daily Gold ETL pipeline
- Documentation: Root causes and solutions documented for operational handoff

**System Ready for Yellow Phase** (Dimensional Data Warehouse Design)

---

**Note:** Earlier versions included a persistent `dim_datetime` table with a dedicated generation job. During Green/Yellow optimization, we removed the persistent time dimension; time attributes are now computed on-the-fly during Spark transformations and persisted in Gold/Postgres as needed, reducing storage overhead and complexity.

---
---

# Phase 4: Data Warehouse Design (The Yellow Phase)

## Decision: Star Schema & Kimball Model

**Requirement:** The BI team needs performant read access for dashboards answering questions like:
- "Top 10 buyers per region?"
- "Sales distribution by product category?"
- "Revenue trends by month?"

**Decision:** Implemented a **Star Schema** using the Kimball methodology in Hive database (`ecommerce_dw`).

**Architecture:**
- **Fact Table:** `fact_transactions` — Contains metrics (quantity, revenue) and foreign keys to dimensions
- **Dimension Tables:** Denormalized tables surrounding the fact for optimized joins
  - `dim_products` — Product details with price history
  - `dim_customers` — Customer demographics
  - `dim_dates` — Time dimensions for temporal analysis
  - `dim_countries` — Geographic and regional data

**Rationale:** Star schema provides the fastest query performance for BI tools while maintaining data integrity through conformed dimensions.

---

## Decision: Handling Price History (SCD Type 2)

**Requirement:** Answer historical pricing questions:
- "What was the price for product X in September?"
- "How has pricing evolved over time?"

**Challenge:** In a normalized `transactions_raw` table, `stock_code` (Business Key) is not unique if a product has multiple price versions over time. Using `stock_code` directly loses historical accuracy.

**Solution:** Introduced a **Surrogate Key** architecture:

1. **Surrogate Key (`product_sk`)** in `dim_products`
   - Unique identifier generated for each version of a product
   - Example: Product "Widget-A" version 1 has `product_sk=101`, version 2 has `product_sk=102`

2. **Fact table references `product_sk`** instead of `stock_code`
   - A transaction from September always points to `product_sk=101` (Widget-A at September price)
   - A transaction from November always points to `product_sk=102` (Widget-A at November price)

3. **Historical accuracy guaranteed**
   - Every transaction points to the *exact version* of the product (and price) that existed at the moment of purchase
   - `dim_products` maintains `effective_date` and `end_date` for version tracking

**Benefit:** BI queries automatically get historically accurate pricing without complex date filtering logic.

---

## Decision: Partitioning Strategy

**Requirement:** The dataset will grow indefinitely with hourly ingestions (~24 new partitions per day).

**Challenge:** Without partitioning, every query scans millions of rows unnecessarily, causing poor dashboard performance.

**Solution:** Partitioned `fact_transactions` by **Year and Month**:

```sql
CREATE TABLE fact_transactions (
    transaction_id INT,
    product_sk INT,
    customer_sk INT,
    date_sk INT,
    quantity INT,
    unit_price FLOAT,
    total_amount FLOAT
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;
```

**Benefits:**
- **Partition Pruning:** Hive skips irrelevant partitions during query execution
  - Query: "Sales in October 2024" only reads `year=2024, month=10` partition
  - Result: 95%+ faster than scanning full table
- **Data Lifecycle Management:** Easy to archive or delete old partitions
- **Incremental Loading:** Daily/hourly jobs load into specific year/month partitions

**Example Query Performance:**
```sql
-- Without partition pruning: scans entire table (~billions of rows)
SELECT SUM(total_amount) FROM fact_transactions WHERE invoice_date >= '2024-10-01';

-- With partition pruning: scans only October 2024 partition (~millions of rows)
SELECT SUM(total_amount) FROM fact_transactions 
WHERE year=2024 AND month=10;
```

---

## Yellow Phase Status

**Completed Successfully**

- Star schema deployed in `ecommerce_dw` database
- Fact table (`fact_transactions`) designed with proper grain
- Dimension tables created with conformed attributes
- SCD Type 2 implemented for `dim_products` with surrogate keys
- Partitioning strategy applied (Year/Month on `fact_transactions`)
- Database views created for BI team convenience
- Query optimization validated with sample dashboards

**System Ready for Purple Phase** (Anomaly Detection & Alerting)

---

# Engineering Diary: Purple Phase

## Anomaly Detection & Statistical Monitoring

### Requirement
Detect potential fraud and data quality issues in real-time order processing to protect business integrity and identify operational inefficiencies.

### Challenge: Defining "Anomalous"
**Problem:** Traditional rule-based thresholds (e.g., "flag if quantity > 1000") are brittle and require manual tuning. Different products have vastly different demand patterns (e.g., high-volume commodity vs. specialty item).

**Solution:** Adopted **dynamic Z-Score thresholds** computed per product:
- For each `stock_code`, calculate the **median** and **standard deviation** of historical order quantities
- Flag orders where `quantity > median + (2 × std_dev)` — this captures the top ~2.3% of the distribution
- Thresholds automatically adapt to product popularity and seasonality without manual recalibration

### Implementation: PySpark Job (`detect_anomalies.py`)

**Architecture:**
1. **Load Gold Data:** Read `fact_transactions` and historical baselines from Postgres
2. **Compute Baselines:** Use Spark SQL to calculate `percentile_approx(0.5)` and `stddev` per product
3. **Score Orders:** Apply Z-score formula to each transaction
4. **Persist Results:** Write flagged anomalies to `ecommerce_gold.anomalies` table with detailed attributes
5. **Email Alerts:** Trigger HTML-formatted email notifications to data stewards via Gmail SMTP

**Credential Security:** SMTP credentials are stored in a local `.env` file (Git-ignored) and loaded at job startup, ensuring secrets never appear in code or version control.

### Postgres Schema for Anomalies

```sql
CREATE TABLE anomalies (
    anomaly_id SERIAL PRIMARY KEY,
    order_id TEXT,
    stock_code TEXT,
    quantity INT,
    median_qty FLOAT,
    std_deviation FLOAT,
    z_score FLOAT,
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'NEW', -- NEW | REVIEWED | RESOLVED | FALSE_POSITIVE
    investigator_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Jenkins Orchestration

- **Schedule:** Runs immediately after the Gold ETL completes (post-`run_fact_postgres.sh`)
- **Dependencies:** Requires successful `fact_transactions` load
- **Output:** Logs captured in Jenkins UI; anomalies queryable in Postgres

### BI Visualization (Tableau)

Connected directly to Postgres Gold Layer, stakeholders can:
- **Monitor Trends:** Time-series view of anomaly frequency and Z-score distribution
- **Product Analysis:** Drill into specific products to review flag history and baseline metrics
- **Geographic Insights:** Map anomalies by country/region to identify localized issues
- **Investigation Workflow:** Mark investigations as REVIEWED, RESOLVED, or FALSE_POSITIVE to refine baselines

### Feedback Loop & Continuous Improvement

**False Positives:** When legitimate bulk orders are mistakenly flagged, users mark them as FALSE_POSITIVE in Tableau. The system uses this feedback to refresh baselines and reduce noise.

**True Anomalies:** Genuinely suspicious orders are investigated by the fraud team; resolved cases are marked as RESOLVED and archived for historical analysis and model tuning.

**Seasonal Adjustment:** Monthly baseline refresh jobs recompute medians and standard deviations to capture seasonal demand patterns (e.g., holiday spikes, summer dips).

---

## Purple Phase Status

**Completed Successfully**

- Z-score anomaly detection engine deployed and operational
- PySpark job (`detect_anomalies.py`) integrated into Jenkins post-Gold ETL
- Email alerting via Gmail SMTP with secure credential management
- Postgres `anomalies` table with full audit trail and investigation workflow
- Tableau dashboards connected to Gold Layer for stakeholder monitoring
- Data quality feedback loop established (false positive / true positive triage)
- Baseline refresh strategy documented and scheduled

**System Ready for Blue Phase** (Advanced BI Dashboards & Operational Monitoring)
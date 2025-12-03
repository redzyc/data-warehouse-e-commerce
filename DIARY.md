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
| **Green** | Next Phase | Data mart creation (aggregations, fact tables) |
| **Yellow** | Planned | Dimensional warehouse modeling (star schema) |
| **Purple** | Planned | Alerting system & SLA monitoring |
| **Blue** | Planned | BI dashboards & stakeholder visualization |

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

**System Ready for Green Phase** (Data Mart ETL & Aggregation)
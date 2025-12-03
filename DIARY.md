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

## ⚡ Spark & Hive Data Processing

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

**Completed Successfully** ✓

- Docker infrastructure stable with persistent storage
- Spark cluster properly configured and operational
- Hive metastore connected and queryable
- Jenkins orchestration automated with scheduled jobs
- Data integrity constraints enforced (FK validation)
- Three core ingestion pipelines operational (logs, products, countries)
- SCD Type 0 & Type 2 implementations working correctly

**System Ready for Green Phase** (Data Marts & Aggregations)
# E-Commerce Data Warehouse Pipeline

An end-to-end **data engineering pipeline** for e-commerce analytics, built with **Docker**, **Apache Spark**, **Hive**, and **Jenkins** for automated orchestration.

**Status:** Red Phase Completed (Infrastructure & Data Ingestion Layer)

---

## Table of Contents
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Key Concepts](#key-concepts)
- [Development](#development)

---

## 🛠 Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Containerization** | Docker & Docker Compose | Latest |
| **Distributed Storage** | Apache Hadoop HDFS | 3.2.1 |
| **Data Warehouse** | Apache Hive | 2.3.2 |
| **Processing Engine** | Apache Spark | 3.5.0 |
| **Job Orchestration** | Jenkins | Latest |
| **Language** | Python (PySpark) | 3.x |
| **Metadata Store** | PostgreSQL | 9.6+ |

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
├── DIARY.md                           # Engineering decisions & troubleshooting log
├── README.md                          # This file
└── LICENSE
```

---

## Data Flow & Pipelines

### Data Pipeline Schedule

| Job | Frequency | Triggers | Output |
|-----|-----------|----------|--------|
| **Setup_Infrastructure** | On-demand | Manual trigger via Jenkins | Initialize database & HDFS |
| **Hourly_Data_Pipeline** | Every hour | Cron schedule | Generates and ingests raw logs |
| **Daily_Data_Pipeline** | Every morning (6 AM) | Cron schedule | Refreshes products & countries |

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

## 🔑 Key Concepts & Patterns

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

### Troubleshooting

See **DIARY.md** for detailed engineering decisions and known issues:
- Hive metastore persistence on Docker restart
- Spark hanging with "Initial job has not accepted resources"
- Container timezone & DNS resolution

---

## Project Phases

| Phase | Status | Scope |
|-------|--------|-------|
| **Red** | Completed | Infrastructure setup & data ingestion |
| **Green** | Next | Create data marts (aggregations) |
| **Yellow** | Planned | Build dimensional data warehouse |
| **Purple** | Planned | Implement alerting & SLA monitoring |
| **Blue** | Planned | BI dashboard & finalization |

---

## Additional Resources

- **Engineering Log:** See `DIARY.md` for detailed problem-solving notes and design decisions
- **Spark Documentation:** [Apache Spark 3.5.0](https://spark.apache.org/docs/3.5.0/)
- **Hive Documentation:** [Apache Hive 2.3.2](https://cwiki.apache.org/confluence/display/Hive)
- **Docker Reference:** [Docker Compose](https://docs.docker.com/compose/)

---

**Repository:** `data-warehouse-e-commerce` | **Branch:** `feature/red-phase`

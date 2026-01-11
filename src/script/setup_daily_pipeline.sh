#!/bin/bash
#
# Setup script for daily data pipeline initialization
# Generates products, countries, and transaction logs data
# Should be run ONCE before the hourly pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "Daily Data Pipeline Setup"
echo "=========================================="

# Step 1: Initialize Hive database and tables
echo ""
echo "[1/5] Creating Hive database and tables..."
hive -f "$SCRIPT_DIR/init_hive.sql" 2>&1 | grep -E "CREATE|DROP|Compiling|OK|ERROR" || true
echo "✓ Hive database initialized"

# Step 2: Generate products
echo ""
echo "[2/5] Generating products data..."
cd "$SCRIPT_DIR"
spark-submit --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    processing/products/ingest_products.py 2>&1 | tail -5
echo "✓ Products generated"

# Step 3: Generate countries  
echo ""
echo "[3/5] Generating countries data..."
spark-submit --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    processing/countries/ingest_countries.py 2>&1 | tail -5
echo "✓ Countries generated"

# Step 4: Generate transaction logs
echo ""
echo "[4/5] Generating transaction logs..."
spark-submit --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    processing/logs/ingest_logs.py 2>&1 | tail -5
echo "✓ Transaction logs generated"

echo ""
echo "=========================================="
echo "✓ Setup Complete!"
echo "=========================================="

#!/bin/bash
JOB_NAME="Daily Country and Product Generator"
PYTHON_SCRIPT="/opt/spark/scripts/product_and_countries_generator.py"
SPARK_MASTER="spark://spark-master:7077"
LOG_FILE="/opt/spark/logs/gen_products_and_country_$(date +%Y%m%d%H%M%S).log"
OUTPUT_DIR_COUNTRY="/user/root/ecommerce/countries/countries_$(date +%Y-%m-%d)"
OUTPUT_DIR_PRODUCT="/user/root/ecommerce/countries/countries_$(date +%Y-%m-%d)"

mkdir -p $(dirname $LOG_FILE)

START_TIME=$(date +%s)

/opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    $PYTHON_SCRIPT 2>&1 | tee $LOG_FILE


EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

if [ $EXIT_CODE -eq 0 ]; then
    echo " SUCCESS: $JOB_NAME finished for $DURATION seconds." | tee -a $LOG_FILE
else
    echo " FAILURE: $JOB_NAME  (Exit Code $EXIT_CODE). Duration: $DURATION seconds." | tee -a $LOG_FILE
    echo " Check log files for more information: $LOG_FILE" | tee -a $LOG_FILE
    exit 1
fi
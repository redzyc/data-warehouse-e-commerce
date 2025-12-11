#!/bin/bash

JOB_NAME="4-Hourly Ingest Transaction"
PYTHON_SCRIPT="/opt/spark/jobs/logs/ingest_logs.py" 
SPARK_MASTER="spark://spark-master:7077"
LOG_FILE="/opt/spark/logs/transactions_$(date +%Y%m%d%H%M%S).log"
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
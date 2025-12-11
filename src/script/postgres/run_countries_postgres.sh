#!/bin/bash

JOB_NAME="Daily Country Postgres"
PYTHON_SCRIPT="/opt/spark/jobs/countries/postgres_countries.py" 
SPARK_MASTER="spark://spark-master:7077"
LOG_FILE="/opt/spark/logs/countries_$(date +%Y%m%d%H%M%S).log"
JDBC_DRIVER_PATH="/opt/spark/jars-custom/postgresql-42.7.8.jar"
mkdir -p $(dirname $LOG_FILE)

START_TIME=$(date +%s)

/opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.host=spark-master \
    --deploy-mode client \
    --jars $JDBC_DRIVER_PATH \
    --conf spark.driver.extraClassPath=$JDBC_DRIVER_PATH \
    --conf spark.executor.extraClassPath=$JDBC_DRIVER_PATH \
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
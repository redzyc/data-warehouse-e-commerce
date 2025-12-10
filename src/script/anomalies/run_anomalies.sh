#!/bin/bash

PYTHON_SCRIPT="/opt/spark/jobs/anomalies/anomalies_data.py"
SPARK_MASTER="spark://spark-master:7077"
LOG_FILE="/opt/spark/logs/anomalies_$(date +%Y%m%d%H%M%S).log"
JDBC_DRIVER_PATH="/opt/spark/jars-custom/postgresql-42.7.8.jar"

if [ -f /opt/spark/.env ]; then
    export $(cat /opt/spark/.env | xargs)
fi


mkdir -p $(dirname $LOG_FILE)

/opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --jars $JDBC_DRIVER_PATH \
    --conf spark.driver.extraClassPath=$JDBC_DRIVER_PATH \
    --conf spark.executor.extraClassPath=$JDBC_DRIVER_PATH \
    $PYTHON_SCRIPT 2>&1 | tee $LOG_FILE

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo " SUCCESS: Analysis complete." | tee -a $LOG_FILE
else
    echo " FAILURE: Analysis failed." | tee -a $LOG_FILE
    exit 1
fi
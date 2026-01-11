import check_db
from common_imports import *
import write_to_db

HDFS_INPUT_PATH=os.getenv("HDFS_INPUT_PATH") + "/logs/*"
HIVE_TARGET_TABLE = os.getenv("HIVE_TARGET_TABLE") + ".transactions_raw"


def ingest_logs():
    spark = create_spark_session_hive()

    try:
        #1 Extract
        df_raw = check_db.is_empty(spark, HDFS_INPUT_PATH)
        if not df_raw:
            return
        #2 Transform
        df_columns = df_raw.withColumn(
            "temp_cols", split(trim(col("value")), ",")
        ).select(
            col("temp_cols").getItem(0).alias("invoice_no"),
            col("temp_cols").getItem(1).alias("stock_code"),
            col("temp_cols").getItem(2).alias("quantity_str"),
            col("temp_cols").getItem(3).alias("invoice_date_str"),
            col("temp_cols").getItem(4).alias("customer_id"),
            col("temp_cols").getItem(5).alias("country_id")
        ).drop("temp_cols")
        
        df_final = df_columns.select(
            col("invoice_no"),
            col("stock_code"),
            col("quantity_str").cast(IntegerType()).alias("quantity"),
            to_timestamp(col("invoice_date_str"), "yyyy-MM-dd HH:mm:ss").alias("invoice_date"),
            col("customer_id"),
            substring(col("country_id"), 1, 4).alias("country_id"),
            substring(col("country_id"), -1, 1).alias("region_code"),
            current_timestamp().alias("ingestion_timestamp")
        ).filter(col("quantity").isNotNull()
        )
        #Load
        write_to_db(df_final, HIVE_TARGET_TABLE, mode="append")
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    ingest_logs()
from common_imports import *
from continent import continent_fun
from write_to_db import write_to_db


TODAY_TIMESTAMP =  (datetime.now()).strftime("%Y-%m-%d")
HDFS_NAMENODE_URI ='hdfs://namenode:9000'
HDFS_INPUT_PATH =f"{HDFS_NAMENODE_URI}/user/root/ecommerce/countries/countries_{TODAY_TIMESTAMP}"
HIVE_TARGET_TABLE="ecommerce_db.countries" 

def ingest_countries():
    spark = create_spark_session_hive()

    try:
        #1 Exctract
        df_raw = spark.read.text(HDFS_INPUT_PATH)
        
        if df_raw.rdd.isEmpty():
            spark.stop()
            return

        #2 Transform
        df_parsed = df_raw.withColumn(
            "temp_cols", split(trim(col("value")), ",")
        ).select(
            trim(col("temp_cols").getItem(0)).alias("country_key_raw"), 
            trim(col("temp_cols").getItem(1)).alias("country_name")
        )
        df_final = df_parsed.select(
            (col("country_key_raw")).alias("country_id"),
            col("country_name")
        ).withColumn(
            "continent", continent_fun(col("country_id"))
        )

        #3 Load
        write_to_db(df_final, HIVE_TARGET_TABLE, mode="append")
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    ingest_countries()
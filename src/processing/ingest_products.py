from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType, DateType


TODAY_TIMESTAMP =  (datetime.now()).strftime("%Y-%m-%d")
HDFS_NAMENODE_URI ='hdfs://namenode:9000'
HDFS_INPUT_PATH =f"{HDFS_NAMENODE_URI}/user/root/ecommerce/products/products_{TODAY_TIMESTAMP}"
HIVE_TARGET_TABLE="ecommerce_db.products" 

def create_spark_session():
    return SparkSession.builder \
        .appName("IngestProducts_Transactions") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .enableHiveSupport() \
        .getOrCreate()

def ingest_product():
    spark = create_spark_session()

    try:
        #1 Extract
        df_raw = spark.read.text(HDFS_INPUT_PATH)

        if df_raw.rdd.isEmpty():
            spark.stop()
            return
        #2 Transform
        df_new = df_raw.withColumn(
            "temp_cols", split(trim(col("value")), ",")
        ).select(
            col("temp_cols").getItem(0).alias("stock_code"),
            col("temp_cols").getItem(1).alias("description"),
            col("temp_cols").getItem(2).cast(FloatType()).alias("unit_price_new"),
            to_date(col("temp_cols").getItem(3), "yyyy-MM-dd").alias("price_date_new")
        ).drop("temp_cols")

        df_exisiting = spark.table(HIVE_TARGET_TABLE)

        window_deduplicated = Window.partitionBy("stock_code").orderBy(col("price_date_new").desc())

        df_new = df_new.withColumn("x", row_number().over(window_deduplicated)).filter(col("x")==1).drop("x")

        window_latest = Window.partitionBy("stock_code").orderBy(col("price_date").desc())

        df_exisiting_final = df_exisiting.withColumn("x", row_number().over(window_latest)) \
                                        .filter(col("x") == 1) \
                                        .select(col("stock_code"), col("unit_price").alias("unit_price_latest"))



        df_final = df_new.join(
            df_exisiting_final, 
            on="stock_code", 
            how="left"
        )

        df_final = df_final.filter(
            (col("unit_price_latest").isNull()) | 
            (col("unit_price_new") != col("unit_price_latest"))
        ).select(
            col("stock_code"), 
            col("description"), 
            col("unit_price_new").alias("unit_price"), 
            col("price_date_new").alias("price_date")
        )
        
        #Load
        df_final.write \
            .mode("append") \
            .insertInto(HIVE_TARGET_TABLE)
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    ingest_product()
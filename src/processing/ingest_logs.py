from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

HDFS_NAMENODE_URI ='hdfs://namenode:9000'
HDFS_INPUT_PATH =f"{HDFS_NAMENODE_URI}/user/root/ecommerce/logs/*"
HIVE_TARGET_TABLE="ecommerce_db.transactions_raw"

def create_spark_session():
    return SparkSession.builder \
        .appName("IngestLogs_Transactions") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .enableHiveSupport() \
        .getOrCreate()
def ingest_logs():
    spark = create_spark_session()

    try:
        #1 Extract
        df_raw = spark.read.text(HDFS_INPUT_PATH)

        if df_raw.rdd.isEmpty():
            spark.stop()
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
            col("temp_cols").getItem(5).alias("country_field")
        ).drop("temp_cols")

        df_parsed_country = df_columns \
            .withColumn("country_parts", split(col("country_field"), "-")) \
            .withColumn("country_id", col("country_parts").getItem(0)) \
            .withColumn("region_id", col("country_parts").getItem(1).cast(IntegerType())) \
            .drop("country_field", "country_parts")
        
        df_final = df_parsed_country.select(
            col("invoice_no"),
            col("stock_code"),
            col("quantity_str").cast(IntegerType()).alias("quantity"),
            to_timestamp(col("invoice_date_str"), "yyyy-MM-dd HH:mm:ss").alias("invoice_date"),
            col("customer_id"),
            col("country_id"),
            col("region_id"),
            current_timestamp().alias("ingestion_timestamp")
        ).filter(col("quantity").isNotNull()
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
    ingest_logs()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window 
from pyspark.sql.types import IntegerType, DateType, FloatType
from datetime import datetime, timedelta

TODAY_TIMESTAMP =  (datetime.now()).strftime("%Y-%m-%d")
HDFS_NAMENODE_URI ='hdfs://namenode:9000'
HDFS_INPUT_PATH =f"{HDFS_NAMENODE_URI}/user/root/ecommerce/countries/countries_{TODAY_TIMESTAMP}"
HIVE_TARGET_TABLE="ecommerce_db.countries" 

def create_spark_session():
    return SparkSession.builder \
        .appName("IngestCountries_SCD0") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.legacy.allowSelfGenJoin", "true") \
        .enableHiveSupport() \
        .getOrCreate()
def continent_fun(id):
    continent = substring(id,1,2)
    return when(continent == "11", lit("Europe")) \
           .when(continent == "22", lit("Africa")) \
           .when(continent == "33", lit("Asia")) \
           .when(continent == "44", lit("Australia and Oceania")) \
           .when(continent == "55", lit("North America")) \
           .when(continent == "66", lit("South America")) \
           .when(continent == "13", lit("Transcontinental (Europe/Asia)")) \
           .when(continent == "10", lit("Europe/Global Territories (UK, France)")) \
           .when(continent == "50", lit("North America/Global Territories (USA)")) \
           .otherwise(lit("UNKNOWN"))

def ingest_countries():
    spark = create_spark_session()

    try:
        #1 Exctract
        df_raw = spark.read.text(HDFS_INPUT_PATH)
        
        if df_raw.rdd.isEmpty():
            print("Nema novih fajlova. Zavrsavam.")
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
            col("country_name"),
            substring(col("country_key_raw"), 6, 2).cast("int").alias("region_id") 
        ).withColumn(
            "continent", continent_fun(col("country_id"))
        )
        df_final.show()

        #3 Load
        df_final.write \
            .mode("overwrite") \
            .insertInto(HIVE_TARGET_TABLE)
        
        spark.sql(f"MSCK REPAIR TABLE {HIVE_TARGET_TABLE}")
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    ingest_countries()
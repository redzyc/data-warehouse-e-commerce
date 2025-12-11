import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

PG_URL = "jdbc:postgresql://hive-metastore-postgresql:5432/ecommerce_gold"
PG_PROPERTIES = {
    "user": "hive", 
    "password": "hive", 
    "driver": "org.postgresql.Driver"
}


def create_spark_session():
    return SparkSession.builder \
        .appName("Postgres_Products") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.driver.host", "spark-master") \
        .enableHiveSupport() \
        .getOrCreate()


def process_countries(spark):
        df_raw = spark.table("ecommerce_db.products")

        df_final = df_raw.select(
            col("product_id"),
            col("stock_code"), 
            col("description"), 
            col("unit_price"), 
            col("price_date")
            )
        df_final.write \
        .jdbc( \
            url=PG_URL, \
            table="dim_products", \
            mode="overwrite", \
            properties=PG_PROPERTIES)


if __name__ == "__main__":
      spark = create_spark_session()
      process_countries(spark)
      spark.stop()



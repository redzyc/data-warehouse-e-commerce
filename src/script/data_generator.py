import os
from pyspark.sql import SparkSession
import random
from datetime import datetime
from datetime import timedelta

HDFS_NAMENODE_URI = "hdfs://namenode:9000"
HDFS_OUTPUT_PATH = f"{HDFS_NAMENODE_URI}/user/root/ecommerce/logs/"
EXISTING_PRODUCT_CODES=["10001","10002","10003","10004","10005","10006","10007","10008","10009","10010"]
EXISITING_COUNTRY_IDS=["1101","1102","1001","1002","5001","1301","1302","3301","4401","6601"]
HIVE_DB = "ecommerce_db"

def get_spark_session():
    return SparkSession.builder \
        .appName("ProductAndCountriesGenerator") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .enableHiveSupport() \
        .getOrCreate()  

def fetch_data_from_db(spark):
    countries = []
    products = []
    try:
        if spark.catalog.tableExists(f"{HIVE_DB}.products"):
            df_prod = spark.sql(f"SELECT DISTINCT stock_code FROM {HIVE_DB}.products")
            products = [row['stock_code'] for row in df_prod.collect()]
            
        if spark.catalog.tableExists(f"{HIVE_DB}.countries"):
            df_count = spark.sql(f"SELECT DISTINCT country_id FROM {HIVE_DB}.countries")
            countries = [row['country_id'] for row in df_count.collect()]
            
    except Exception as e:
        print(f"Error during reading files from Hive: {e}")

    if not products:
        products = EXISTING_PRODUCT_CODES

    if not countries:
        countries = EXISITING_COUNTRY_IDS      


    return products, countries
    



def generate_rdd(valid_products, valid_countries):
    #Invoice No, Stock Code, Quantity, Inovice date, Customer ID, CountryID, Country
    invoice_no =random.randint(100000,999999)
    stock_code = random.choice(valid_products)
    country_id = random.choice(valid_countries)
    quantity = random.randint(1,100)
    random_date = datetime.now() - timedelta(minutes=random.randint(0, 240))
    invoice_date = random_date.strftime("%Y-%m-%d %H:%M:%S")
    customer_id = random.randint(10000,99999)
    region_id = random.randint(1,6)
    country = f"{country_id}-{region_id}"
    return f"{invoice_no},{stock_code},{quantity},{invoice_date},{customer_id},{country}"
    

if __name__ == "__main__":
    spark = get_spark_session()
    sc = spark.sparkContext

    valid_products, valid_countries = fetch_data_from_db(spark)


    rdd = sc.parallelize([generate_rdd(valid_products, valid_countries) for x in range(100)])
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-00")
    output_path = f"{HDFS_OUTPUT_PATH}logs_{timestamp}"
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    hdfs_path = hadoop.fs.Path(output_path)

    if fs.exists(hdfs_path):
        fs.delete(hdfs_path, True)
    rdd.coalesce(1).saveAsTextFile(output_path)
    spark.stop()
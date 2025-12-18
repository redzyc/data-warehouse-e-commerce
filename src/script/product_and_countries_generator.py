import os
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

HDFS_NAMENODE_URI = "hdfs://namenode:9000"
HDFS_OUTPUT_PATH = f"{HDFS_NAMENODE_URI}/user/root/ecommerce/"

#CountryID, Country
COUNTRY_CATALOG = [
    ("11", "Germany"),
    ("11", "France"),
    ("11", "Italy"),
    ("13", "Turkey"), 
    ("50", "USA"),   
    ("44", "Australia"),
    ("33", "China")       
]
#Stock Code, Product Description, Unit Price, Date
PRODUCT_CATALOG = [
    ("10001", "Laptop", 1196.99),
    ("10002", "Smartphone", 399.99),
    ("10003", "Tablet", 399.99),
    ("10004", "Headphones", 359.99),
    ("10005", "Smartwatch", 249.99),
    ("10006", "Camera", 549.99),
    ("10007", "Printer", 149.99),
    ("10008", "Monitor", 299.99),
    ("10009", "Keyboard", 89.99),
    ("10010", "Mouse", 49.99),
]

def get_spark_session():

    return SparkSession.builder \
        .appName("ProductAndCountriesGenerator") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .enableHiveSupport() \
        .getOrCreate()   

def generate_country():
    #CountryID, Country
    data_lines = []
    for i, (continent_code, country_name) in enumerate(COUNTRY_CATALOG, start=1):
        country_suffix = f"{i:02d}"
        base_country_id = f"{continent_code}{country_suffix}"
        for region_number in range(1, 7):
            country_id = f"{base_country_id}-{region_number}"
            data_lines.append(f"{country_id},{country_name}")
    return data_lines


def generate_product():
    #Stock Code, Product Description, Unit Price, Date
    data_lines = []
    today_str = (datetime.now()).strftime("%Y-%m-%d")
    for stock_code, description, unit_price in PRODUCT_CATALOG:
        random_change_price= random.uniform(-0.3, 0.3)
        unit_price = round(unit_price *(1+ random_change_price), 2)
        data_lines.append(f"{stock_code},{description},{unit_price},{today_str}")
    return data_lines

if __name__ == "__main__":


    spark = get_spark_session()
    sc = spark.sparkContext
    timestamp = (datetime.now()).strftime("%Y-%m-%d")


    country_data = generate_country()
    rdd = sc.parallelize(country_data)
    output_path = os.path.join(HDFS_OUTPUT_PATH,"countries", f"countries_{timestamp}")
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    hdfs_path = hadoop.fs.Path(output_path)

    if fs.exists(hdfs_path):
        fs.delete(hdfs_path, True)
    rdd.coalesce(1).saveAsTextFile(output_path)


    product_data = generate_product()
    rdd = sc.parallelize(product_data)
    output_path = os.path.join(HDFS_OUTPUT_PATH,"products", f"products_{timestamp}")
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    hdfs_path = hadoop.fs.Path(output_path)

    if fs.exists(hdfs_path):
        fs.delete(hdfs_path, True)
    rdd.coalesce(1).saveAsTextFile(output_path)


    spark.stop()




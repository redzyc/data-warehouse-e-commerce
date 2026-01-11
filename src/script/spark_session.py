from pyspark.sql import SparkSession
def create_spark_session_hive():

    return SparkSession.builder \
        .appName("ProductAndCountriesGenerator") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .enableHiveSupport() \
        .getOrCreate() 

def create_spark_session_postgres():
    return SparkSession.builder \
        .appName("Fact_Postgres") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.driver.host", "spark-master") \
        .enableHiveSupport() \
        .getOrCreate()


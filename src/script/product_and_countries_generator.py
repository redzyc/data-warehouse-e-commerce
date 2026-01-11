from common_imports import *

HDFS_NAMENODE_URI = "hdfs://namenode:9000"
HDFS_OUTPUT_PATH = f"{HDFS_NAMENODE_URI}/user/root/ecommerce/"

#CountryID, Country
COUNTRY_CATALOG = [
    ("11", "Germany"),
    ("11", "France"),
    ("11", "Spain"),
    ("11", "Italy"),
    ("13", "Turkey"), 
    ("22", "Nigeria"),
    ("22", "South Africa"),
    ("33", "India"),
    ("33", "Japan"),
    ("55", "Canada"),
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

def generate_country():
    #CountryID, Country
    data_lines = []
    for i, (continent_code, country_name) in enumerate(COUNTRY_CATALOG, start=1):
        country_suffix = f"{i:02d}"
        country_id = f"{continent_code}{country_suffix}"
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


def save_to_hdfs(data, folder_name, filename_prefix, spark, timestamp):
        sc = spark.sparkContext
        rdd = sc.parallelize(data)
        output_path = os.path.join(HDFS_OUTPUT_PATH, folder_name, f"{filename_prefix}_{timestamp}")
        
        hadoop = spark._jvm.org.apache.hadoop
        fs = hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        hdfs_path = hadoop.fs.Path(output_path)
        
        if fs.exists(hdfs_path):
            fs.delete(hdfs_path, True)
        
        rdd.coalesce(1).saveAsTextFile(output_path)
        
if __name__ == "__main__":


    spark = create_spark_session_hive()
    sc = spark.sparkContext
    timestamp = (datetime.now()).strftime("%Y-%m-%d")


    
    country_data = generate_country()
    save_to_hdfs(country_data, "countries", "countries", spark, timestamp)
    
    product_data = generate_product()
    save_to_hdfs(product_data, "products", "products", spark, timestamp)


    spark.stop()




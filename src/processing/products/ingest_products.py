from common_imports import *
from spark_session import create_spark_session_hive
import check_db

TODAY_TIMESTAMP =  (datetime.now()).strftime("%Y-%m-%d")
HDFS_INPUT_PATH = os.path.join(HDFS_OUTPUT_PATH, "products", f"products_{TODAY_TIMESTAMP}")
HIVE_TARGET_TABLE=os.getenv("HIVE_TARGET_TABLE")+".products"


def ingest_product():
    spark = create_spark_session_hive()

    try:
        #1 Extract
        df_raw = check_db.is_empty(spark, HDFS_INPUT_PATH)
        if not df_raw:
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
        try:
            table_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {HIVE_TARGET_TABLE}").collect()[0]['cnt']
            is_empty = table_count == 0
        except:
            is_empty = True

        if is_empty:
            df_final = df_new.withColumn(
                "product_id",
                concat_ws("-", col("stock_code"), col("price_date_new"))
            ).select(
                col("product_id"),
                col("stock_code"), 
                col("description"), 
                col("unit_price_new").alias("unit_price"), 
                col("price_date_new").alias("price_date")
            )
        else:
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
            df_final = df_final.withColumn(
                "product_id",
                concat_ws("-", col("stock_code"), col("price_date_new"))
            )

            df_final = df_final.filter(
                (col("unit_price_latest").isNull()) | 
                (col("unit_price_new") != col("unit_price_latest"))
            ).select(
                col("product_id"),
                col("stock_code"), 
                col("description"), 
                col("unit_price_new").alias("unit_price"), 
                col("price_date_new").alias("price_date")
            )
        
        #Load
        df_final.createOrReplaceTempView("temp_products")
        spark.sql(f"INSERT INTO TABLE {HIVE_TARGET_TABLE} SELECT * FROM temp_products")
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    ingest_product()
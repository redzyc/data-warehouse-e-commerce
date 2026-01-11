from common_imports import *
from write_to_db import write_to_postgres


PG_URL = os.getenv("PG_URL")
PG_PROPERTIES = {
    "user": os.getenv("PG_USER"), 
    "password": os.getenv("PG_PASSWORD"), 
    "driver": os.getenv("PG_DRIVER", "org.postgresql.Driver")
}

def etl_fact_transactions():
    spark = create_spark_session_postgres()

    try:
        df_raw = spark.table(f"{HIVE_DB}.transactions_raw")
        if df_raw.rdd.isEmpty():
            return
        
        df_transactions = df_raw.select(
            col("invoice_no").cast("string"),
            col("stock_code").cast("string"),
            col("customer_id").cast("string"),
            col("country_id").cast("string"),
            col("region_code").cast("string"),
            col("quantity").cast(IntegerType()),
            col("invoice_date"),
            to_date(col("invoice_date")).alias("raw_date")
        )

        df_products = spark.table("ecommerce_db.products")
        df_countries = spark.table("ecommerce_db.countries") \
            .select(col("country_id").cast("string")) \
            .withColumn("is_valid_country", lit(True))
        
        df_validated = df_transactions.join(
            df_countries,
            on="country_id",
            how="left"
        )
        df_errors = df_validated.filter(col("is_valid_country").isNull()).select(
            col("invoice_no"),
            col("stock_code"),
            col("customer_id"),
            col("quantity").cast(IntegerType()),
            col("invoice_date"),
            col("country_id"),
            col("region_code"),
            lit("Missing Country ID").alias("error_message"),
            lit(0).alias("retry_count"),
            current_timestamp().alias("ingestion_timestamp")
        )

        if df_errors.count() > 0:
            print(f" Found {df_errors.count()} transactions with invalid Country ID. Writing to transactions_error.")
            write_to_postgres(df_errors, "transactions_error", PG_URL, PG_PROPERTIES, mode="overwrite")


        df_valid = df_validated.filter(col("is_valid_country") == True)
        df_joined = df_valid.alias("t").join(
            df_products.alias("p"),
            on="stock_code",
            how="left"
        )
        df_candidates = df_joined.filter(
            col("p.price_date") <= col("t.raw_date")
        )

        window_spec = Window \
            .partitionBy("t.invoice_no", "t.stock_code", "t.customer_id") \
            .orderBy(col("p.price_date").desc())

        df_windowed = df_candidates.withColumn("rn", row_number().over(window_spec)) \
                                      .filter(col("rn") == 1) \
                                      .drop("rn")
        df_final = df_windowed.select(
            concat_ws("-", col("t.invoice_no"), col("t.invoice_date")).alias("transaction_id"),
            col("t.invoice_no"),
            col("p.product_id").alias("product_id"),
            col("t.country_id"),
            col("t.region_code"),
            col("t.customer_id"),
            col("t.quantity").cast(IntegerType()),
            coalesce(col("p.unit_price"), lit(0.0)).cast(FloatType()).alias("unit_price"),
            (col("t.quantity").cast(IntegerType()) * coalesce(col("p.unit_price"), lit(0.0)).cast(FloatType())).alias("total_amount"),
            lit("$").alias("currency"),
            col("t.invoice_date"),
            current_timestamp().alias("ingestion_timestamp")
        )
        if df_final.count() > 0:
            print(f"Found {df_final.count()} valid transactions. Writing to fact_transactions table.")
            write_to_postgres(df_final, "fact_transactions", PG_URL, PG_PROPERTIES, mode="overwrite")
    except Exception as e:
        print(f" ETL Failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    etl_fact_transactions()
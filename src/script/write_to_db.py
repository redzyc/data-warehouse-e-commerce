from pyspark.sql import SparkSession
def write_to_postgres(df_final, table_name, pg_url, pg_properties, mode="overwrite"):
    df_final.write \
        .jdbc( \
            url=pg_url, \
            table=table_name, \
            mode=mode, \
            properties=pg_properties)

def write_to_db(df, table_name, mode="overwrite", format="parquet"):
    df.createOrReplaceTempView("temp_view")
    if mode == "overwrite":
        spark = SparkSession.getActiveSession()
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM temp_view")
    else:
        spark = SparkSession.getActiveSession()
        spark.sql(f"INSERT INTO TABLE {table_name} SELECT * FROM temp_view")


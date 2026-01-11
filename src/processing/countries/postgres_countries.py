from common_imports import *
from write_to_db import write_to_postgres


# PostgreSQL credentials from environment variables
PG_URL = os.getenv("PG_URL")
PG_PROPERTIES = {
    "user": os.getenv("PG_USER"), 
    "password": os.getenv("PG_PASSWORD"), 
    "driver": os.getenv("PG_DRIVER", "org.postgresql.Driver")
}



def process_countries(spark):
        df_raw = spark.table("ecommerce_db.countries")

        df_final = df_raw.select(
                col("country_id"),
                col("country_name"),
                col("continent")
            )
        write_to_postgres(df_final, "dim_countries", PG_URL, PG_PROPERTIES, mode="overwrite")

if __name__ == "__main__":
      spark = create_spark_session_postgres()
      process_countries(spark)
      spark.stop()



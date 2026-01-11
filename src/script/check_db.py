def is_empty(spark, path):
    df_raw = spark.read.text(path)
            
    if df_raw.rdd.isEmpty():
        spark.stop()
        return
    return df_raw
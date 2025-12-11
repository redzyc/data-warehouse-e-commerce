import sys
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SENDER_EMAIL = os.getenv("SMTP_SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SMTP_APP_PASSWORD")
RECEIVER_EMAIL = os.getenv("SMTP_RECEIVER_EMAIL")

ENABLE_REAL_EMAIL = True

def create_spark_session():
    return SparkSession.builder \
        .appName("Purple_Phase_Anomaly_Detection") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.driver.host", "spark-master") \
        .enableHiveSupport() \
        .getOrCreate()

def send_alert_email(anomalies_data):
    if not anomalies_data:
        print("No anomalies to report.")
        return
        
    subject = "Alarm: Anomalies in orders Detected"

    body = "The following orders are > 2 Standard Deviations above the median quantity:\n\n"
    body += "{:<15} {:<15} {:<15} {:<10} {:<10} {:<10}\n".format("Invoice", "Customer", "Stock Code", "Qty", "Median", "Limit")
    body += "-" * 90 + "\n"

    for row in anomalies_data:
        body += "{:<15} {:<15} {:<15} {:<10} {:<10.2f} {:<10.2f}\n".format(
            str(row['invoice_no']), 
            str(row['customer_id']), 
            str(row['stock_code']), 
            str(row['quantity']),
            float(row['median_qty']),
            float(row['median_stddev'])
        )

    print("\n" + "="*50)
    print(f"EMAIL PREVIEW TO: {RECEIVER_EMAIL}")
    print(body)
    print("="*50 + "\n")

    if ENABLE_REAL_EMAIL and SENDER_EMAIL and SENDER_PASSWORD:
        try:
            msg = MIMEMultipart()
            msg['From'] = SENDER_EMAIL
            msg['To'] = RECEIVER_EMAIL
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            text = msg.as_string()
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, text)
            server.quit()
            print("Email sent successfully via SMTP.")
        except Exception as e:
            print(f"Failed to send email: {e}")
    else:
        print("Email sending skipped (Check .env or ENABLE_REAL_EMAIL)")
    

def detect_anomalies():
    spark = create_spark_session()
    print("Analyzing data for anomalies...")

    try:
        df = spark.table("ecommerce_db.transactions_raw")

        df_stats = df.groupBy("stock_code").agg(
            median("quantity").alias("median_qty"),
            stddev("quantity").alias("stddev_qty")
        )

        df_joined = df.join(
            df_stats, 
            on="stock_code", 
            how="inner"
        )

        df_final = df_joined.withColumn(
            "median_stddev",
            col("median_qty") + (lit(2) * coalesce(col("stddev_qty"), lit(0)))
        ).filter(
            col("quantity") > col("median_stddev")
        )

        results = df_final.select(
            "invoice_no", "customer_id", "stock_code", "quantity", "median_qty", "median_stddev"
        ).collect()

        data_to_send = [row.asDict() for row in results]

        if len(data_to_send) > 0:
            print(f"Found {len(data_to_send)} anomalies!")
            send_alert_email(data_to_send)
        else:
            print("No anomalies detected. System healthy.")

    except Exception as e:
        print(f"Anomaly detection failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    detect_anomalies()
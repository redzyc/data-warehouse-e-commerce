DROP DATABASE IF EXISTS ecommerce_db CASCADE;
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

CREATE TABLE IF NOT EXISTS transactions_raw (
    invoice_no STRING,
    stock_code STRING,
    quantity INT,
    invoice_date TIMESTAMP,
    customer_id STRING,
    country_id STRING,
    region_id INT ,
    ingestion_timestamp TIMESTAMP
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS countries (
    country_id STRING,
    country_name STRING,
    continent STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS products (
    stock_code STRING,
    description STRING,
    unit_price FLOAT,
    price_date DATE 
)
STORED AS PARQUET;
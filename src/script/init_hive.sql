DROP DATABASE IF EXISTS ecommerce_db CASCADE;
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_countries;
DROP TABLE IF EXISTS transactions_error;
CREATE TABLE IF NOT EXISTS transactions_raw (
    invoice_no STRING,
    stock_code STRING,
    quantity INT,
    invoice_date TIMESTAMP,
    customer_id STRING,
    country_id STRING,
    region_code STRING,
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
    product_id STRING,
    stock_code STRING,
    description STRING,
    unit_price FLOAT,
    price_date DATE 
)
STORED AS PARQUET;
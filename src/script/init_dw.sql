CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

CREATE TABLE IF NOT EXISTS dim_products (
    product_sk STRING,      
    stock_code STRING,      
    description STRING,
    unit_price FLOAT,
    currency STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_countries (
    country_id STRING,       
    country_name STRING,     
    region_code STRING,     
    continent STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dim_datetime (
    date_key INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id STRING,  
    segment STRING,
    first_purchase_date DATE
)
STORED AS PARQUET;



CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id INT,
    invoice_no STRING,
    product_sk STRING,       
    country_id STRING,       
    date_key INT,            
    customer_id STRING,     
    quantity INT,
    unit_price FLOAT,
    total_amount FLOAT,
    
    ingestion_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;
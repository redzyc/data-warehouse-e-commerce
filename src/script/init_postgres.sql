
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_countries;
DROP TABLE IF EXISTS transactions_error;

CREATE TABLE dim_products (
    product_id VARCHAR(255) PRIMARY KEY,
    stock_code VARCHAR(50),
    description TEXT,
    unit_price FLOAT,
    currency VARCHAR(10),
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP
);

CREATE TABLE dim_countries (
    country_id VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(100),
    region_code VARCHAR(50),
    continent VARCHAR(50)
);


CREATE TABLE fact_transactions (
    transaction_id TEXT,
    invoice_no VARCHAR(50),
    product_id VARCHAR(255),
    country_id VARCHAR(10),  
    customer_id VARCHAR(50), 
    quantity INT,
    unit_price FLOAT,
    currency VARCHAR(3) DEFAULT '$',
    total_amount FLOAT,
    invoice_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions_error (
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    customer_id VARCHAR(50),
    quantity INT,
    invoice_date TIMESTAMP,
    country_id VARCHAR(10),
    error_message TEXT,
    retry_count INT DEFAULT 0,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
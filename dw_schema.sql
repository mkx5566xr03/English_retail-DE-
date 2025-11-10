-- 建立資料倉儲 Schema
CREATE SCHEMA IF NOT EXISTS dw;

---------------------------------------------------------
-- 維度表：國家 (Dim_Country)
---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_country (
    country_key SERIAL PRIMARY KEY,
    country_name TEXT UNIQUE
);

---------------------------------------------------------
-- 維度表：產品 (Dim_Product)
---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key SERIAL PRIMARY KEY,
    stock_code TEXT UNIQUE,
    description TEXT
);

---------------------------------------------------------
-- 維度表：顧客 (Dim_Customer)
---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE,
    country_key INT REFERENCES dw.dim_country(country_key)
);

---------------------------------------------------------
-- 維度表：時間 (Dim_Time)
---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_key SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    year INT,
    month INT,
    day_of_week INT
);

---------------------------------------------------------
-- 事實表：銷售 (Fact_Sales)
---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    invoice_no TEXT,
    product_key INT REFERENCES dw.dim_product(product_key),
    customer_key INT REFERENCES dw.dim_customer(customer_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    country_key INT REFERENCES dw.dim_country(country_key),
    quantity INT,
    unit_price FLOAT,
    total_amount FLOAT
);
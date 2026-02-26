-- 03_gold.sql
-- Aggregations and business-ready models (Gold layer)

-- Example placeholder:
-- CREATE OR REPLACE TABLE gold.daily_sales AS
-- SELECT order_date_dt, SUM(amount) AS total_sales
-- FROM silver.sales_clean
-- GROUP BY order_date_dt;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE OR REPLACE TABLE GOLD.SALES_BY_STATE AS
SELECT 
    d.state,
    SUM(f.sales_price) AS total_sales
FROM SILVER.FACT_SALES f
JOIN SILVER.CUSTOMER_DIM d
    ON f.customer_sk = d.customer_sk
GROUP BY d.state;

--Sales by year
CREATE OR REPLACE TABLE GOLD.SALES_BY_YEAR AS
SELECT  
    DATE_TRUNC('year', transaction_date) AS sales_year,
    SUM(sales_price) AS total_sales 
FROM SILVER.FACT_SALES
GROUP BY sales_year;
    
-- Sales by month
CREATE OR REPLACE TABLE GOLD.MONTHLY_SALES AS
SELECT
    DATE_TRUNC('month', transaction_date) AS sales_month,
    SUM(sales_price) AS total_sales     
FROM SILVER.FACT_SALES 
GROUP BY SALES_MONTH;

--Top 10 stores by sales
CREATE OR REPLACE TABLE GOLD.TOP_STORES AS
SELECT 
    store_id,
    SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY store_id           
ORDER BY total_sales DESC
LIMIT 10;
-- 02_silver.sql
-- Transformations and cleansing to Silver layer

-- Example placeholder:
-- CREATE OR REPLACE TABLE silver.sales_clean AS
-- SELECT *, CAST(order_date AS DATE) AS order_date_dt
-- FROM bronze.sales_raw;
use warehouse compute_wh;
CREATE SCHEMA IF NOT EXISTS SILVER;
USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE OR REPLACE TABLE SILVER.STORE_SALES_CLEAN AS
SELECT 
    ss.ss_sold_date_sk,
    ss.ss_store_sk,
    ss.ss_customer_sk,
    ss.ss_sales_price,
    ss.ss_quantity,
from BRONZE.STORE_SALES ss
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM d
ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year BETWEEN 1998 AND 1999
AND ss.ss_sales_price IS NOT NULL
AND ss.ss_quantity > 0;
DESC TABLE SILVER.STORE_SALES_CLEAN;

SELECT MIN(ss_sales_price), MAX(ss_sales_price)
FROM SILVER.STORE_SALES_CLEAN;

SELECT COUNT(*) AS TOTAL_ROWS
FROM SILVER.STORE_SALES_CLEAN;
SELECT COUNT(*) AS TOTAL_ROWS
FROM BRONZE.STORE_SALES;

SELECT COUNT(*) 
FROM SILVER.STORE_SALES_CLEAN
WHERE ss_sales_price IS NULL;

SELECT COUNT(*) 
FROM SILVER.STORE_SALES_CLEAN
WHERE ss_quantity <=0;
use warehouse compute_wh;
use database snowflake_learning_db;

create schema if not exists silver;

CREATE OR REPLACE TABLE SILVER.STORE_SALES_CLEAN AS
SELECT 
    ss.ss_sold_date_sk,
    ss.ss_store_sk,
    ss.ss_customer_sk,
    ss.ss_sales_price,
    ss.ss_quantity,
from BRONZE.STORE_SALES ss
join snowflake_sample_data.tpcds_sf100tcl.date_dim dd
on ss.ss_sold_date_sk = dd.d_date_sk
where dd.d_year between 1998 and 1999
and ss.ss_sales_price is not null
and ss.ss_quantity > 0;


--WHERE ss.ss_sales_price is not null and ss.ss_quantity > 0;
desc table silver.store_sales_clean;
SELECT 
    MIN(ss_sales_price) AS min_price,
    MAX(ss_sales_price) AS max_price
FROM silver.store_sales_clean;


--validate the silver layer
select count(*) from bronze.store_sales;
select count(*) from silver.store_sales_clean;

select count(*) from silver.store_sales_clean where ss_sales_price is null;

select count(*) from silver.store_sales_clean where ss_quantity <= 0;
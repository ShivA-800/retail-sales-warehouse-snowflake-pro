USE database snowflake_sample_data;
desc table tpcds_sf100tcl.store_sales;
select count(*) from tpcds_sf100tcl.store_sales;

use warehouse compute_wh;
use database snowflake_learning_db;

create schema if not exists bronze;



create or replace table bronze.store_sales as
select * from snowflake_sample_data.tpcds_sf100tcl.store_sales limit 500000000;


--phase 3: data validation(completmeness, accuracy, consistency, timeliness, uniqueness)

select count(*) from bronze.store_sales;
desc table bronze.store_sales;

SELECT 
COUNT(*) AS TOTAL_ROWS,
COUNT(SS_SALES_PRICE) AS NON_NULL_SALES_PRICE,
FROM BRONZE.STORE_SALES;
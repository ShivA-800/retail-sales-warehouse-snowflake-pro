use warehouse compute_wh;

create schema if not exists gold;


create or replace table gold.montly_sales as 
select
    date_trunc('month',transactions) as month,
    sum(sales_price) as total_sales
from silver.fact_sales
group by sales_month;
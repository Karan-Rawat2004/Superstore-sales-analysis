-- Project: Superstore Sales Analysis
-- Objective: Load and analyze sales data
-- Includes: Data cleaning, table design, ETL, EDA
-- Author: Karan Rawat
-- Date: 30 june 2025

-- Set the active database.
use superstore_sales_data; 

-- creating a sample table to insert all the data from the csv file 
CREATE TABLE staging_sales (
  row_id INT,
  order_id VARCHAR(50),
  order_date DATE,
  ship_date DATE,
  ship_mode VARCHAR(50),
  customer_id VARCHAR(50),
  customer_name VARCHAR(100),
  segment VARCHAR(50),
  country VARCHAR(50),
  city VARCHAR(50),
  state VARCHAR(50),
  region VARCHAR(50),
  product_id VARCHAR(50),
  category VARCHAR(50),
  sub_category VARCHAR(50),
  sales DECIMAL(10,2)
); 
select count(*) from staging_sales;

-- Inserting data to dimensional tables.
-- Inserting data to customer table. 
INSERT IGNORE INTO customers (customer_id, customer_name, segment, country,
 city, state, region)
SELECT DISTINCT customer_id, customer_name, segment, country, city, state, region
FROM staging_sales;

-- Reviewing the customer table.
select count(*) from customers;


-- Inserting data to orders. 
INSERT INTO orders (order_id, orderDate, ship_date, ship_mode)
SELECT DISTINCT order_id, order_date, ship_date, ship_mode
FROM staging_sales; 

-- Reviewing the orders table
select count(*) from orders; 


-- Inserting data to product table 
INSERT INTO products (product_id, category, sub_category) 
SELECT DISTINCT product_id, category, sub_category 
from staging_sales;

-- Reviewing the products table.
select count(*) from products;

-- Inserting data to order_detail  table.
INSERT INTO order_details (row_id, order_id, product_id, customer_id, sales)
SELECT row_id, order_id, product_id, customer_id, sales
FROM staging_sales;

-- Reviewing the order_detail table.
select count(*) from order_details;  

-- EDA section 
-- Exploratory Data Analysis (EDA) Queries 

-- 1) Total no. of orders 
select count(distinct order_id) as total_orders from orders;

-- 2) Total no. of customers 
select count(distinct customer_id) as total_customer from customers; 
  
  -- 3) Total no. of products
select count(distinct product_id) as total_products from products; 
 
 -- 4) total sales 
 select sum(sales) as total_sales from order_details;
 
 -- 5) sales by region 
 select c.region,sum(o.sales) as total_sales
 from customers c
 inner join order_details o 
 on c.customer_id=o.customer_id 
 group by region ; 
 
-- 6) sales by year 
select year(o.orderdate) as year, sum(od.sales) as total_Sales 
from orders o 
inner join order_details od 
on o.order_id= od.order_id 
group by year(orderdate); 

-- 7) Top 5 customers by sales 
select c.customer_name, sum(o.sales) 
from customers c
inner join order_details o 
on c.customer_id = o.customer_id 
group by customer_name
order by sum(sales) desc
limit 5; 

-- 8) Orders by ship mode
select  ship_mode, count(order_id) 
from orders 
group by ship_mode; 

-- 9)sales by category 
SELECT p.category, SUM(od.sales) AS total_sales
FROM order_details od
JOIN products p ON od.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;  



-- Section: Advanced queries for business insights

-- These help identify trends, opportunities,
-- and areas for improvement. 

-- 1) Top 5 most profitable products 
select p.category, p.sub_category, p.product_id ,sum(o.sales) as total_sales
from products p
inner join order_details o 
on p.product_id= o.product_id 
group by category, sub_category, product_id
order by sum(sales) desc 
limit 5 ; 

-- 2)Monthly sales and running total using window function 
-- Shows trends + helps forecast next months.
select date_format(o.orderDate, '%Y-%m') as year_months,
sum(od.sales) as total_sales, sum(sum(od.sales)) over (order by date_format(o.orderDate, '%Y-%m')) as running_total
from orders o
inner join order_details od
on o.order_id= od.order_id
group by year_months;

-- 3) Customers whose total sales are above average (using subquery) 
-- Finding high value customers for loyalty program.
select c.customer_name,c.customer_id, sum(o.sales) as total_sales
from customers c 
inner join order_details o 
on c.customer_id= o.customer_id 
group by customer_name,customer_id
having total_sales>(select avg(sales) from order_details);


-- 4)Top sub-category by sales in each region using CTE + RANK() 
with cte as (
select sub_category,region, sum(sales) as total_sales,
rank() over(partition by region order by sum(sales) desc) as rnk
from products p 
inner join order_details o 
on p.product_id=o.product_id
inner join customers c 
on o.customer_id= c.customer_id
group by region, sub_category
) 
select sub_category,region, total_sales
from cte
where rnk=1;

-- 5)Number of repeat and one time customer 
-- shows customer loyalty and retention 
select 
case 
when order_count>1 then 'repeat customer'
else  'one time customer' 
end as customer_type ,
count(*) from
(select customer_id,count(distinct order_id) as order_count
 from order_details
group by customer_id )t
group by customer_type ;


-- 6) Year-over-year sales growth using window function 
-- Shows business growth trend over year
with yearly_sales as (
select year(o.orderdate) as years, sum(od.sales) as total_sales
from orders o
inner join order_details od 
on o.order_id= od.order_id 
group by year(orderdate)
) 
select years, total_sales, 
lag(total_sales) over (order by years) as last_year_sales,
round((total_sales - lag(total_sales) over (order by years))/lag(total_sales) 
over (order by years) * 100,2) as yoy_growth 
from  yearly_sales ;


-- 7) average delivery time per ship mode.
select ship_mode, 
round(avg(datediff(ship_date,orderdate)), 2) 
as avg_delivery_days
from orders
group by ship_mode 
order by avg_delivery_days ;  


-- 8) Top 3 city by sales in each region.(using window function and subquery)
select city,region, total_sales from ( 
select c.city,region, sum(o.sales) as total_sales ,
row_number() over (partition by c.region order by sum(o.sales) desc) as top_city 
from customers c 
inner join order_details o
on c.customer_id= o.customer_id
group by city,region
) t 
where top_city between 1 and 3 ;


-- 9)Find top 1% of orders by revenue using window function + CTE 
WITH order_totals AS (
  SELECT order_id,SUM(sales) AS order_sales
  FROM order_details
  GROUP BY order_id
),
ranked_orders AS (
  SELECT 
    order_id,
    order_sales,
    NTILE(100) OVER (ORDER BY order_sales DESC) AS percentile_rank
  FROM 
    order_totals
)
SELECT 
  order_id,
  order_sales
FROM 
  ranked_orders
WHERE 
  percentile_rank = 1
ORDER BY 
  order_sales DESC;







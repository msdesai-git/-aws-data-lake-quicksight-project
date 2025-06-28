-- Create an external Athena table over the cleaned Parquet data, partitioned by region
CREATE EXTERNAL TABLE IF NOT EXISTS quickkart_db.cleaned_orders (
  order_id INT,
  order_date STRING,
  customer_name STRING,
  product STRING,
  category STRING,
  amount INT
)
PARTITIONED BY (
  region STRING
)
STORED AS PARQUET
LOCATION 's3://quickkart-clean-aws/orders/2025/06/';

-- Sync the table with new partitions stored in S3 (like refreshing metadata)
MSCK REPAIR TABLE quickkart_db.cleaned_orders; 

-- Analyze total sales and order count per region, sorted by highest sales
SELECT region, COUNT(*) AS order_count, SUM(amount) AS total_sales
FROM quickkart_db.cleaned_orders
GROUP BY region
ORDER BY total_sales DESC;
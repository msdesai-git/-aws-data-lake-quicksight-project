from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from Glue Catalog
csv_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="quickkart_db",
    table_name="raw_sales_day1_csv"
)

json_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="quickkart_db",
    table_name="raw_sales_day2_json"
)

# Convert to DataFrames
csv_df = csv_dyf.toDF()
json_df = json_dyf.toDF()

# Align schema
ordered_cols = ["order_id", "order_date", "customer_name", "product", "category", "amount", "region"]
csv_df = csv_df.select(ordered_cols)
json_df = json_df.select(ordered_cols)

# Union
combined_df = csv_df.unionByName(json_df)

# Convert back to DynamicFrame
combined_dyf = DynamicFrame.fromDF(combined_df, glueContext, "combined_dyf")

# Write to S3 with partitioning
glueContext.write_dynamic_frame.from_options(
    frame=combined_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://quickkart-clean-aws/orders/2025/06/",
        "partitionKeys": ["region"]
    },
    format="parquet"
)
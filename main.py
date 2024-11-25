# Import necessary libraries
import pandas as pd
from dotenv import load_dotenv
from query_tool import DatabaseQueryTool
from utils import read_sql_file
import os

# Load environment variables
load_dotenv('config/.env')

# Load configurations from environment variables
snowflake_config = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

bigquery_config = {
    "project": os.getenv("BIGQUERY_PROJECT_ID"),
    "credentials": os.getenv("BIGQUERY_CREDENTIALS_FILE"),
}

# Initialize the query tool
query_tool = DatabaseQueryTool(snowflake_config, bigquery_config)

# Read SQL queries from files
snowflake_query = read_sql_file("queries/snowflake_query.sql")
bigquery_query = read_sql_file("queries/bigquery_query.sql")

# Execute query on BigQuery
print("Running BigQuery query...")
bigquery_df = query_tool.query_bigquery(bigquery_query)
print(f"BigQuery query completed. Retrieved {len(bigquery_df)} rows.")

# Execute query on Snowflake
print("Running Snowflake query...")
snowflake_df = query_tool.query_snowflake(snowflake_query)
print(f"Snowflake query completed. Retrieved {len(snowflake_df)} rows.")

# Inner Join Results
print("Performing left join on orders, users, and items as skus...")
join_columns = ["order_id", "user_id", "item_sku"]
output_columns = [
    "event_timestamp_utc", "order_id", "user_id", "event_action", "item_sku", "item_price", "traffic_source",
    "user_country", "device_category"
]

result_df = query_tool.join_results(bigquery_df, snowflake_df, join_columns, output_columns)

# Output results
print("Left join completed. Here are the first few rows of the result:")
print(result_df.head())

# Add a new column to the result - if the event_action is "Add to Cart", then the new column "is_purchase" should be 1, otherwise 0.
result_df["is_purchase"] = result_df["event_action"].apply(lambda x: 1 if x == "Add to Cart" else 0)

# Write the result_df to a Snowflake table named "orders_items_events"
table_name = '"orders_items_events"'
query_tool.write_to_snowflake(result_df, table_name)

## Import logs
import_logs = query_tool.import_logs
import_logs

## Join logs
join_logs = query_tool.join_logs
join_logs

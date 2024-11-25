import os
from dotenv import load_dotenv
from query_tool import DatabaseQueryTool, read_sql_file

# Load environment variables
load_dotenv()

# Load configurations from environment variables
snowflake_config = {
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "account": os.getenv("account"),
    "warehouse": os.getenv("warehouse"),
    "database": os.getenv("database"),
    "schema": os.getenv("schema"),
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

# Execute queries
snowflake_df = query_tool.query_snowflake(snowflake_query)
bigquery_df = query_tool.query_bigquery(bigquery_query)

# Join results
join_columns = ["Order_ID"]
output_columns = [
    "Order_ID", "Customer_ID", "Total_Amount", "Product_Count",
    "Product_Category", "Viewed_Products", "Last_View_Timestamp"
]
result_df = query_tool.join_results(snowflake_df, bigquery_df, join_columns, output_columns)

# Output the joined results
print(result_df)

# Example of writing to Snowflake
# Make sure to specify the table name where you want to insert the data
table_name = "joined_data_table"
query_tool.write_to_snowflake(result_df, table_name)

# Optionally: Save the logs as CSV files for further review
query_tool.import_logs.to_csv("import_logs.csv", index=False)
query_tool.join_logs.to_csv("join_logs.csv", index=False)

# Output logs for review
print("Import Logs:")
print(query_tool.import_logs.tail())

print("Join Logs:")
print(query_tool.join_logs.tail())

# Close connections
query_tool.close_connections()

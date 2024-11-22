import os
import pandas as pd
from google.cloud import bigquery
import snowflake.connector
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class DatabaseQueryTool:
    def __init__(self, snowflake_config, bigquery_config):
        self.snowflake_config = snowflake_config
        self.bigquery_config = bigquery_config
        
        # Initialize connections as None (they will be created on demand)
        self.snowflake_conn = None
        self.bigquery_client = None

        # Set up logging
        logging.basicConfig(level=logging.INFO)

    def get_bigquery_client(self):
        if not self.bigquery_client:
            logging.info("Initializing BigQuery client...")
            self.bigquery_client = bigquery.Client.from_service_account_json(self.bigquery_config["credentials"])
        return self.bigquery_client

    def get_snowflake_connection(self):
        if not self.snowflake_conn:
            logging.info("Initializing Snowflake connection...")
            self.snowflake_conn = snowflake.connector.connect(
                user=self.snowflake_config["user"],
                password=self.snowflake_config["password"],
                account=self.snowflake_config["account"],
                warehouse=self.snowflake_config["warehouse"],
                database=self.snowflake_config["database"],
                schema=self.snowflake_config["schema"]
            )
        return self.snowflake_conn

    def query_bigquery(self, query):
        client = self.get_bigquery_client()
        query_job = client.query(query)
        result = query_job.result()
        df = result.to_dataframe()
        return df

    def query_snowflake(self, query):
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        cursor.close()
        return df

    def join_results(self, df1, df2, join_columns, output_columns):
        logging.info("Joining results...")
        joined_df = pd.merge(df1, df2, on=join_columns, how='inner')[output_columns]
        return joined_df

    def close_connections(self):
        if self.snowflake_conn:
            logging.info("Closing Snowflake connection...")
            self.snowflake_conn.close()
        if self.bigquery_client:
            logging.info("Closing BigQuery client...")
            self.bigquery_client.close()

# Function to read SQL from a file
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

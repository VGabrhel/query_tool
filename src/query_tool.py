import os
import pandas as pd
import time
from google.cloud import bigquery
import snowflake.connector
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv('config/.env')

class DatabaseQueryTool:
    """
    A utility class for interacting with databases. 
    Currently, only Snowflake and BigQuery are supported.
    Provides methods for querying data, managing database connections, 
    and joining query results.

    Attributes:
        snowflake_config (dict): Configuration parameters for Snowflake connection.
        bigquery_config (dict): Configuration parameters for BigQuery client.
        snowflake_conn: Snowflake connection object (initialized on demand).
        bigquery_client: BigQuery client object (initialized on demand).
    """
    def __init__(self, snowflake_config, bigquery_config):
        """
        Initializes the DatabaseQueryTool with Snowflake and BigQuery configurations.

        Args:
            snowflake_config (dict): Snowflake configuration parameters (user, password, account, etc.).
            bigquery_config (dict): BigQuery configuration parameters (credentials, etc.).
        """
        self.snowflake_config = snowflake_config
        self.bigquery_config = bigquery_config
        
        # Initialize connections as None (they will be created on demand)
        self.snowflake_conn = None
        self.bigquery_client = None

        # DataFrames to store logs
        self.import_logs = pd.DataFrame(
            columns=["source", "query", "rows", "columns", "data_mb", "time_sec", "timestamp"]
        )
        self.join_logs = pd.DataFrame(
            columns=["df1_shape", "df2_shape", "join_columns", "output_columns", "result_shape", "duplicate_rows", "timestamp"]
        )

        # Set up logging
        logging.basicConfig(level=logging.INFO)

    def get_bigquery_client(self):
        """
        Initializes and returns a BigQuery client if not already initialized.

        Returns:
            bigquery.Client: The BigQuery client object.
        """
        if not self.bigquery_client:
            logging.info("Initializing BigQuery client...")
            self.bigquery_client = bigquery.Client.from_service_account_json(self.bigquery_config["credentials"])
        return self.bigquery_client

    def get_snowflake_connection(self):
        """
        Initializes and returns a Snowflake connection if not already initialized.

        Returns:
            snowflake.connector.SnowflakeConnection: The Snowflake connection object.
        """
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

    def write_to_snowflake(self, df, table_name):
        """
        Writes a pandas DataFrame to a Snowflake table with lowercase column names.

        Args:
            df (pd.DataFrame): The pandas DataFrame to write.
            table_name (str): The name of the Snowflake table where the data will be written.
        """
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()

        try:
            # Convert datetime columns to string before insertion
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):  # Check if the column is a timestamp/datetime
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string format

            # Handle NaN values by replacing them with None (interpreted as NULL in Snowflake)
            df = df.where(pd.notnull(df), None)

            # Convert column names to lowercase
            df.columns = [col.lower() for col in df.columns]

            # Check if the table exists, create it if it doesn't (with lowercase column names)
            create_table_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (" + \
                ", ".join([f'"{col}" STRING' for col in df.columns]) + ")"
            cursor.execute(create_table_statement)

            # Convert DataFrame to list of tuples (for insertion)
            data = df.values.tolist()

            # Prepare insert statement (with lowercase column names)
            columns = ', '.join([f'"{col}"' for col in df.columns])
            values_placeholder = ', '.join(['%s'] * len(df.columns))
            insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

            # Execute the insert
            cursor.executemany(insert_statement, data)

            logging.info(f"Successfully inserted {len(df)} rows into Snowflake table '{table_name}'.")

        except Exception as e:
            logging.error(f"Error while writing data to Snowflake: {str(e)}")
        finally:
            cursor.close()

        # Return DataFrame with lowercase column names for verification
        return df

    def query_bigquery(self, query):
        """
        Executes a SQL query on BigQuery and retrieves the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pandas.DataFrame: Query result as a DataFrame.
        """
        try:
            logging.info("Executing query on BigQuery...")
            start_time = time.time()

            # Initialize the BigQuery client
            client = self.get_bigquery_client()
            query_job = client.query(query)

            # Wait for the query to complete and fetch results
            result = query_job.result()
            query_time = time.time() - start_time

            # Convert results to a DataFrame
            df = result.to_dataframe()

            # Fetch statistics from the QueryJob object
            row_count = len(df)
            col_count = len(df.columns)
            transmitted_bytes = query_job.total_bytes_billed or 0  # Handle None gracefully
            transmitted_mb = transmitted_bytes * 0.000001

            # Prepare the new log entry
            new_log_entry = pd.DataFrame(
                [
                    {
                        "source": "BigQuery",
                        "query": query,
                        "row_count": row_count,
                        "col_count": col_count,
                        "data_mb": transmitted_mb,
                        "time_sec": query_time,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                ]
            )

            # Safely concatenate logs
            if self.import_logs is None or self.import_logs.empty:
                self.import_logs = new_log_entry
            else:
                self.import_logs = pd.concat(
                    [self.import_logs, new_log_entry], ignore_index=True
                )

            # Log statistics
            logging.info(
                f"BigQuery: Rows={row_count}, Columns={col_count}, Data={transmitted_mb:.2f} MB, "
                f"Time={query_time:.2f} seconds."
            )

            return df

        except ImportError as e:
            logging.error(
                "BigQuery Storage API module is not installed. "
                "Run 'pip install google-cloud-bigquery-storage' for faster query execution."
            )
            raise e

        except Exception as e:
            logging.error(f"An error occurred while executing the BigQuery query: {e}")
            raise e

    def query_snowflake(self, query):
        """
        Executes a SQL query on Snowflake and retrieves the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pandas.DataFrame: Query result as a DataFrame.
        """
        logging.info("Executing query on Snowflake...")
        start_time = time.time()

        conn = self.get_snowflake_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(query)

            # Fetch results into a DataFrame
            data = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            df = pd.DataFrame(data, columns=columns)

            query_time = time.time() - start_time

            # Create the log DataFrame
            log_entry = pd.DataFrame(
                [
                    {
                        "source": "Snowflake",
                        "query": query,
                        "row_count": len(df),
                        "col_count": len(df.columns),
                        "data_mb": None,  # No data size
                        "time_sec": query_time,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                ]
            )

            # Drop columns that are all NA or empty to avoid the FutureWarning
            log_entry = log_entry.dropna(axis=1, how='all')

            # Concatenate the log entry to the main logs DataFrame
            self.import_logs = pd.concat([self.import_logs, log_entry], ignore_index=True)

            # Log statistics
            logging.info(
                f"Snowflake Query Completed: Rows={len(df)}, Columns={len(df.columns)}, "
                f"Processing Time={query_time:.2f} seconds."
            )

        finally:
            cursor.close()

        return df

    def join_results(self, df1, df2, join_columns, output_columns):
        """
        Joins two pandas DataFrames on specified columns, logs statistics, and stores results in a log DataFrame.

        Args:
            df1 (pd.DataFrame): The first DataFrame.
            df2 (pd.DataFrame): The second DataFrame.
            join_columns (list): List of column names to join on.
            output_columns (list): List of column names to include in the output.

        Returns:
            pandas.DataFrame: The joined DataFrame with selected output columns.
        """
        logging.info("Joining results...")

        # Log initial shapes of DataFrames
        initial_df1_shape = df1.shape
        initial_df2_shape = df2.shape
        logging.info(f"Initial df1 shape: {initial_df1_shape}, df2 shape: {initial_df2_shape}")

        # Convert the data types of join columns to the same type if necessary
        df1[join_columns] = df1[join_columns].astype(str)  # Convert to string
        df2[join_columns] = df2[join_columns].astype(str)  # Convert to string

        # Check for duplicated columns in both DataFrames
        duplicated_columns_df1 = df1.columns[df1.columns.duplicated()].tolist()
        duplicated_columns_df2 = df2.columns[df2.columns.duplicated()].tolist()

        if duplicated_columns_df1:
            logging.warning(f"Duplicated columns in df1: {duplicated_columns_df1}")
        if duplicated_columns_df2:
            logging.warning(f"Duplicated columns in df2: {duplicated_columns_df2}")

        # Perform the join
        joined_df = pd.merge(df1, df2, on=join_columns, how='inner')

        # Check for duplicated rows in the joined DataFrame
        duplicated_rows_count = joined_df.duplicated().sum()
        if duplicated_rows_count > 0:
            logging.warning(f"Number of duplicated rows in joined DataFrame: {duplicated_rows_count}")
        else:
            logging.info("No duplicated rows found in joined DataFrame.")

        # Determine final shape of the joined DataFrame
        result_shape = joined_df.shape
        logging.info(f"Shape of joined DataFrame: {result_shape}")

        # Select specified output columns
        result_df = joined_df[output_columns]
        logging.info(f"Shape of final output DataFrame: {result_df.shape}")

        # Log statistics into the join_logs DataFrame
        log_entry = {
            "df1_shape": initial_df1_shape,
            "df2_shape": initial_df2_shape,
            "join_columns": join_columns,
            "output_columns": output_columns,
            "result_shape": result_shape,
            "duplicated_rows": duplicated_rows_count,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        self.join_logs = pd.concat(
            [self.join_logs, pd.DataFrame([log_entry])],
            ignore_index=True
        )

        return result_df

    def close_connections(self):
        """
        Closes the Snowflake connection and the BigQuery client.
        """
        if self.snowflake_conn:
            logging.info("Closing Snowflake connection...")
            self.snowflake_conn.close()
        if self.bigquery_client:
            logging.info("Closing BigQuery client...")
            self.bigquery_client.close()
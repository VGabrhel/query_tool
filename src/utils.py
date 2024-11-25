# utils.py

import os
import pandas as pd
from datetime import datetime
import time
from dotenv import load_dotenv
from google.cloud import bigquery
import snowflake.connector

def load_env_variables(env_file="config/.env"):
    """
    Loads environment variables from the specified .env file.
    
    Args:
        env_file (str): Path to the .env file. Default is ".env".
    
    Returns:
        bool: True if the environment variables are loaded successfully, False otherwise.
    """
    load_dotenv(env_file)
    required_variables = [
        "user", "password", "account", "warehouse", "database", "schema",
        "BIGQUERY_PROJECT_ID", "BIGQUERY_CREDENTIALS_FILE"
    ]
    
    missing_vars = [var for var in required_variables if not os.getenv(var)]
    
    if missing_vars:
        print(f"Error: Missing environment variables: {', '.join(missing_vars)}")
        return False
    return True

def read_sql_file(file_path):
    """
    Reads a SQL query from a file.
    
    Args:
        file_path (str): Path to the SQL file.
    
    Returns:
        str: The SQL query as a string.
    """
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return ""
    except Exception as e:
        print(f"Error reading SQL file '{file_path}': {e}")
        return ""

def log_dataframe_stats(df, source="Unknown"):
    """
    Logs basic statistics for a DataFrame, including shape and memory usage.
    
    Args:
        df (pandas.DataFrame): The DataFrame to log.
        source (str): The source or context of the DataFrame.
    
    Returns:
        None
    """
    row_count, col_count = df.shape
    memory_usage = df.memory_usage(deep=True).sum() / (1024 ** 2)  # in MB
    print(f"{source} - Rows: {row_count}, Columns: {col_count}, Memory Usage: {memory_usage:.2f} MB")

def save_dataframe_to_csv(df, file_name, append=False):
    """
    Saves a DataFrame to a CSV file, appending if necessary.
    
    Args:
        df (pandas.DataFrame): The DataFrame to save.
        file_name (str): The name of the CSV file.
        append (bool): Whether to append to an existing CSV or overwrite it.
    
    Returns:
        None
    """
    mode = 'a' if append else 'w'
    header = not append
    
    try:
        df.to_csv(file_name, mode=mode, header=header, index=False)
        print(f"Saved DataFrame to {file_name}")
    except Exception as e:
        print(f"Error saving DataFrame to {file_name}: {e}")

def retry_on_failure(func, retries=3, delay=5, *args, **kwargs):
    """
    Retries a function upon failure with a specified delay between attempts.
    
    Args:
        func (function): The function to retry.
        retries (int): The number of retry attempts.
        delay (int): Delay in seconds between retries.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.
    
    Returns:
        Any: The result of the function, or None if all attempts fail.
    """
    attempt = 0
    while attempt < retries:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            time.sleep(delay)
    
    print(f"All {retries} attempts failed.")
    return None

def validate_dataframe_for_snowflake(df, required_columns=None):
    """
    Validates a DataFrame to ensure it has no missing values and the correct columns.
    
    Args:
        df (pandas.DataFrame): The DataFrame to validate.
        required_columns (list): List of required column names. If None, no validation on columns.
    
    Returns:
        bool: True if valid, False otherwise.
    """
    if required_columns:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {', '.join(missing_columns)}")
            return False
    
    if df.isnull().any().any():
        print("Error: DataFrame contains missing values.")
        return False
    
    return True

def format_timestamp(timestamp=None, format="%Y-%m-%d %H:%M:%S"):
    """
    Formats the current timestamp or a provided timestamp into the desired format.
    
    Args:
        timestamp (str/datetime, optional): The timestamp to format. Defaults to None (current timestamp).
        format (str): The format string to use.
    
    Returns:
        str: The formatted timestamp.
    """
    if not timestamp:
        timestamp = datetime.now()
    elif isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    
    return timestamp.strftime(format)

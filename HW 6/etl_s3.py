from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

# Snowflake connection function
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')  # 'snowflake_conn' should be defined in Airflow connections
    conn = hook.get_conn()
    return conn.cursor()

# Task to create tables
@task
def create_tables(con, table1, table2):
    try:
        con.execute("BEGIN")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table1} (
                userId INT NOT NULL,
                sessionId VARCHAR(32) PRIMARY KEY,
                channel VARCHAR(32) DEFAULT 'direct'
            );
        """)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table2} (
                sessionId VARCHAR(32) PRIMARY KEY,
                ts TIMESTAMP
            );
        """)
        con.execute("COMMIT")
        return table1, table2
    except Exception as e:
        con.execute("ROLLBACK")
        print(e)
        raise(e)

# Task to load data from S3 to Snowflake
@task
def load_data(tables, url):
    try:
        con.execute("BEGIN")
        con.execute(f"""
            CREATE OR REPLACE STAGE dev.raw.blob_stage 
            URL = '{url}'
            FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)
        con.execute(f"""
            COPY INTO {tables[0]} FROM @dev.raw.blob_stage/user_session_channel.csv;
        """)
        con.execute(f"""
            COPY INTO {tables[1]} FROM @dev.raw.blob_stage/session_timestamp.csv;
        """)
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(e)
        raise(e)

# Define the DAG
with DAG(
    dag_id='etl_s3',  # The ID for the DAG
    description='ETL Pipeline from Amazon S3 to Snowflake',  # Description of the DAG
    start_date=datetime(2024, 10, 6),  # Start date of the DAG
    catchup=False,  # Don't backfill the DAG
    tags=['ETL'],  # Tags for categorizing the DAG
    schedule_interval='30 15 * * *'  # Schedule the DAG to run at 3:30 PM UTC daily
) as dag:
    
    # Initialize Snowflake connection
    con = return_snowflake_conn()
    
    # Get the URL from Airflow's Variable
    url = Variable.get("url")  # Ensure the "url" variable is set in Airflow
    
    # Define table names
    table1 = "dev.raw.user_session_channel"
    table2 = "dev.raw.session_timestamp"
    
    # Create the tables
    tables = create_tables(con, table1, table2)
    
    # Load the data from S3 into Snowflake
    load = load_data(tables, url)
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

# Function to return a Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Task to create or replace the table in Snowflake
@task
def run_ctas(table, select_sql, primary_key=None):
    logging.info(table)
    logging.info(select_sql)
    
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Check for duplicates based on the primary key
        if primary_key is not None:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            logging.info(sql)
            cur.execute(sql)
            result = cur.fetchone()
            logging.info(f"Primary key check: {result}")
            
            if int(result[1]) > 1:
                logging.error("Primary key uniqueness failed!")
                raise Exception(f"Primary key uniqueness failed: {result}")
                
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error(f'Failed to execute SQL. Completed ROLLBACK! Error: {e}')
        raise

# DAG definition for the ELT process
with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ELT'],
    schedule_interval='45 2 * * *'  # Customize this schedule as needed
) as dag:
    table = "dev.analytics.session_summary"
    select_sql = """SELECT u.*, s.ts FROM dev.raw.user_session_channel u
                    JOIN dev.raw.session_timestamp s ON u.sessionId = s.sessionId"""
    
    # Run the task to create the joined table
    run_ctas(table, select_sql, primary_key='sessionId')
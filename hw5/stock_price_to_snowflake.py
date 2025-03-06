from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="stock_price_to_snowflake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["snowflake"],
)
def stock_price_dag():
    
    @task
    def fetch_stock_data():
        """Fetch stock price data from Alpha Vantage API."""
        symbol = "AAPL"
        api_key = "VBB1QAY8METUTVJZ" 
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact"

        response = requests.get(url)
        data = response.json()

        time_series = data.get("Time Series (Daily)", {})
        ninety_days_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

        results = [
            {
                "symbol": symbol,
                "date": d,
                "open": time_series[d]["1. open"],
                "high": time_series[d]["2. high"],
                "low": time_series[d]["3. low"],
                "close": time_series[d]["4. close"],
                "volume": time_series[d]["5. volume"]
            }
            for d in time_series if d >= ninety_days_ago
        ]

        return results

    @task
    def load_to_snowflake(data):
        """Load stock data into Snowflake, ensuring idempotency."""
        df = pd.DataFrame(data)

        # Secure Snowflake Connection
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # Ensure database and schema selection
        cursor.execute("USE DATABASE DEV;")
        cursor.execute("USE SCHEMA RAW;")

        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS DEV.RAW.STOCK_PRICE (
            SYMBOL STRING,
            DATE DATE,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME INT,
            PRIMARY KEY (SYMBOL, DATE)
        );
        """
        cursor.execute(create_table_sql)

        try:
            cursor.execute("BEGIN;")  # Start Transaction

            # **Delete existing records for full refresh**
            cursor.execute("DELETE FROM DEV.RAW.STOCK_PRICE;")

            # Insert new records
            insert_sql = """
            INSERT INTO DEV.RAW.STOCK_PRICE (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES (%(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s);
            """
            cursor.executemany(insert_sql, df.to_dict("records"))

            # **COMMIT to save changes**
            cursor.execute("COMMIT;")
            cursor.close()

        except Exception as e:
            cursor.execute("ROLLBACK;")  # Undo changes on failure
            raise e

    # DAG Task dependencies
    stock_data = fetch_stock_data()
    load_to_snowflake(stock_data)

stock_price_dag()


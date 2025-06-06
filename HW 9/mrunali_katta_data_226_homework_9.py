# -*- coding: utf-8 -*-
"""Mrunali Katta - DATA 226 - Homework 9.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1luhq0WYKJMp26Ca5WIpPfl-bAmoHBdx4
"""

!wget -nc https://raw.githubusercontent.com/keeyong/sjsu-data226-SP25/refs/heads/main/week13/data/sample_web_log_1.log.gz
!wget -nc https://raw.githubusercontent.com/keeyong/sjsu-data226-SP25/refs/heads/main/week13/data/sample_web_log_2.log.gz
!wget -nc https://raw.githubusercontent.com/keeyong/sjsu-data226-SP25/refs/heads/main/week13/data/sample_web_log_3.log.gz
!wget -nc https://raw.githubusercontent.com/keeyong/sjsu-data226-SP25/refs/heads/main/week13/data/sample_web_log_4.log.gz
!wget -nc https://raw.githubusercontent.com/keeyong/sjsu-data226-SP25/refs/heads/main/week13/data/sample_web_log_5.log.gz
!wget -nc https://raw.githubusercontent.com/keeyong/sjsu-data226-SP25/refs/heads/main/week13/data/sample_web_log_6.log.gz
!wget -nc https://raw.githubusercontent.com/keeyong/sjsu-data226-SP25/refs/heads/main/week13/data/sample_web_log_7.log.gz

!cd /usr/local/lib/python3.11/dist-packages/pyspark/jars && wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.19.0/snowflake-jdbc-3.19.0.jar

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("HandleLogFiles").getOrCreate()

# Load all .gz files in the directory into a DataFrame
df = spark.read.text("*.gz")

# Check the number of partitions
print(df.rdd.getNumPartitions())

df.show(truncate=False)

# Extract the necessary information from log data using regular expressions
pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*" (\d+) (\d+)'

log_df = df.select(
    F.regexp_extract("value", pattern, 1).alias("ip"),
    F.regexp_extract("value", pattern, 2).alias("timestamp"),
    F.regexp_extract("value", pattern, 3).alias("method"),
    F.regexp_extract("value", pattern, 4).alias("url"),
    F.regexp_extract("value", pattern, 5).alias("status").cast("integer"),
    F.regexp_extract("value", pattern, 6).alias("size").cast("integer")
)

log_df.show()

ip_status_count = log_df.groupBy("ip", "status").count().orderBy(F.desc("count"))
ip_status_count.show()

log_df.createOrReplaceTempView("logs")

ip_status_sql = spark.sql("""
    SELECT ip, status, COUNT(*) as count
    FROM logs
    GROUP BY ip, status
    ORDER BY count DESC
""")
ip_status_sql.show()

from google.colab import userdata

account = userdata.get('snowflake_account')
user = userdata.get('snowflake_userid')
password = userdata.get('snowflake_password')
database = "dev"
schema = "analytics"

url = f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}&user={user}&password={password}"

ip_status_sql.write \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", url) \
    .mode("overwrite") \
    .option("dbtable", "ip_status_count") \
    .save()


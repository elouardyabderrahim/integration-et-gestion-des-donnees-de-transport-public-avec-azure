# Databricks notebook source
import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import config

spark.conf.set(
    f"fs.azure.account.key.{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",f"{config.STORAGE_ACCOUNT_KEY}"
)
raw = f"abfss://publictransportdata@{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/"

df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(raw)



# COMMAND ----------

df.dtypes

# COMMAND ----------

from pyspark.sql import functions as f;


# df.withColumn("DepartureTime",f.to_timestamp("DepartureTime"))
df=(df.withColumn('TimeofDepart', f.date_format('DepartureTime', 'HH:mm:ss')))

# COMMAND ----------

from pyspark.sql.functions import col, split

# Split the Date column into Year, Month, and Day columns
date_parts = split(df["Date"], "-")
df = df.withColumn("Year", date_parts[0].cast("int"))
df = df.withColumn("Month", date_parts[1].cast("int"))
df = df.withColumn("Day", date_parts[2].cast("int"))

# Show the resulting DataFrame
display(df)


# COMMAND ----------

df.show()

# COMMAND ----------

# (df.withColumn("arrivaletimeTypetoDate",f.to_timestamp("ArrivalTime"))).show()
df.withColumn("input_arrivaletime",f.to_timestamp("arrivaletimeTypetoDate","HH:mm:ss")).show(truncate=False)

# COMMAND ----------



# Databricks notebook source
import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import config




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



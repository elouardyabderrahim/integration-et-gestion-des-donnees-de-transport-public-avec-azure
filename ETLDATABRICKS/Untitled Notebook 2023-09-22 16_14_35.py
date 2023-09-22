# Databricks notebook source
import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
spark.conf.set(
    f"fs.azure.account.key.bentalebstorageacc.dfs.core.windows.net", 
    "f7yngxT+I8Wzy+J0GAoTGBtjqo92greqVvmofq1zHx8msMTSl33Kws93ICPi6fIAR2z5XFn4t/wD+AStcTUhOQ=="
)
raw = "abfss://publictransportdata@bentalebstorageacc.dfs.core.windows.net/raw/"

df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(raw)

display(df)

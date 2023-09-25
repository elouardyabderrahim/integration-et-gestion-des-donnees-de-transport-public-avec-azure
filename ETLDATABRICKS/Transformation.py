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

display(df)

# Databricks notebook source

mountPoint = "/mnt/publictransportdata/"

spark.conf.set("databricks.spark.dbutils.fs.cp.server-side.enabled", "false")
raw = f"{mountPoint}raw/"
archive = f"{mountPoint}archieve/"

raw_files = dbutils.fs.ls(raw)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]
raw_csv_files.sort()

# delete the oldet file
dbutils.fs.cp(raw_csv_files[0], archive)
print("archeived file: "+ raw_csv_files[0].split("/")[-1].split(".")[0])
dbutils.fs.rm(raw_csv_files[0])

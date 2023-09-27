# Databricks notebook source
import config
# Mounting data lake
storageAccountName = config.STORAGE_ACCOUNT_NAME
storageAccountAccessKey = config.STORAGE_ACCOUNT_KEY
sasToken = config.SAS_TOKEN
blobContainerName = "publictransportdata"
mountPoint = "/mnt/publictransportdata/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

spark.conf.set("databricks.spark.dbutils.fs.cp.server-side.enabled", "false")
raw = f"{mountPoint}raw/"
archive = f"{mountPoint}archive/"

raw_files = dbutils.fs.ls(raw)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]
raw_csv_files.sort()

# delete the oldet file
dbutils.fs.cp(raw_csv_files[0], archive)
dbutils.fs.rm(raw_csv_files[0])
dbutils.fs.unmount("/mnt/publictransportdata/")

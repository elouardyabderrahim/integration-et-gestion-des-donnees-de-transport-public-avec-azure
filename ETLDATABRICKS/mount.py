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

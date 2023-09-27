# Databricks notebook source
# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import config


# COMMAND ----------

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

# COMMAND ----------

start_year = 2021
end_year = 2024

transport_types = ["Bus", "Train", "Tram", "Metro"]
routes = ["Route_" + str(i) for i in range(1, 11)]
stations = ["Station_" + str(i) for i in range(1, 21)]

for year in range(start_year, end_year):
    for month in range(1, 13):  
        # Generate data for the current month
        start_date = datetime(year, month, 1)
        next_month = month + 1 if month < 12 else 12
        day = 1 if month < 12 else 31
        end_date = start_date.replace(day=day, month=next_month) - timedelta(days=1)
        date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]


        # Randomly select 5 days as extreme weather days
        extreme_weather_days = random.sample(date_generated, 5)

        data = []

        for date in date_generated:
            for _ in range(32):  # 32 records per day to get a total of 992 records for each month
                transport = random.choice(transport_types)
                route = random.choice(routes)

                # Normal operating hours
                departure_hour = random.randint(5, 22)
                departure_minute = random.randint(0, 59)

                # Introducing Unusual Operating Hours for buses
                if transport == "Bus" and random.random() < 0.05:  # 5% chance
                    departure_hour = 3

                departure_time = f"{departure_hour:02}:{departure_minute:02}"

                # Normal duration
                duration = random.randint(10, 120)

                # Introducing Short Turnarounds
                if random.random() < 0.05:  # 5% chance
                    duration = random.randint(1, 5)

                # General delay
                delay = random.randint(0, 15)

                # Weather Impact
                if date in extreme_weather_days:
                    # Increase delay by 10 to 60 minutes
                    delay += random.randint(10, 60)

                    # 10% chance to change the route
                    if random.random() < 0.10:
                        route = random.choice(routes)

                total_minutes = departure_minute + duration + delay
                arrival_hour = departure_hour + total_minutes // 60
                arrival_minute = total_minutes % 60
                arrival_time = f"{arrival_hour:02}:{arrival_minute:02}"

                passengers = random.randint(1, 100)
                departure_station = random.choice(stations)
                arrival_station = random.choice(stations)

                data.append([date, transport, route, departure_time, arrival_time, passengers, departure_station, arrival_station, delay])

        # Create a DataFrame for the current month's data
        month_i = str(year) + "_" + str(month).zfill(2)
        df = pd.DataFrame(data, columns=["Date", "TransportType", "Route", "DepartureTime", "ArrivalTime", "Passengers", "DepartureStation", "ArrivalStation", "Delay"])
        df.to_csv("/dbfs/mnt/publictransportdata/raw/rawTransportDataOf_"+ month_i + ".csv" , index=False)

    print(f"Generated data for year - " + str(year))


# COMMAND ----------

dbutils.fs.unmount("/mnt/publictransportdata/")

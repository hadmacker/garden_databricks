# Databricks notebook source
# Tables:
# light
# humidity
# temperature

# COMMAND ----------

import datetime

today = date.today()
year = today.year
month = today.month
day = today.day

print(today)
print(year)
print(month)
print(day)

# COMMAND ----------

# Humidity MTD
df = spark.read.table("humidity")
dfhumidityMtd = df \
  .where(df.year == year) \
  .where(df.month == month) \
  .where(df.day <= day) \
  .sort("timestamp")

# COMMAND ----------

display(dfhumidityMtd)

# COMMAND ----------

# Temperature MTD reports
# * All Data
# * Daily Min Temperature (overnight)
df = spark.read.table("temperature")
dfTempMtd = df \
  .where(df.year == year) \
  .where(df.month == month) \
  .where(df.day <= day) \
  .sort("timestamp")

# COMMAND ----------

display(dfTempMtd)

# COMMAND ----------

display(dfTempMtd)
dfTempMtd.write.mode("overwrite").saveAsTable("temperature_mtd")

# COMMAND ----------

from pyspark.sql.functions import col, date_format, avg, min, max

dfTempMtdMins = dfTempMtd \
  .groupBy("year", "month", "day") \
  .agg(min("temp")) \
  .sort("year", "month", "day")

display(dfTempMtdMins)
#dfTempMtd.write.mode("overwrite").saveAsTable("temperature_mtd")

# COMMAND ----------

# Light MTD reports
df = spark.read.table("light")
dfLightMtd = df \
  .where(df.month == month) \
  .where(df.day <= day) \
  .sort("timestamp")

# COMMAND ----------

# Light, MTD
display(dfLightMtd)

# COMMAND ----------

# Light, all data
df = spark.read.table("light").sort("timestamp")
display(df)

# COMMAND ----------

from pyspark.sql.types import DateType,StructType
from pyspark.sql.functions import col, date_format, avg, min, max,input_file_name

dfEventReports = spark.sql("select * from events") \
  .withColumn("datePart", date_format("epoch", "yyyy-MM-dd")) \
  .withColumn("year", date_format("epoch", "yyyy")) \
  .withColumn("month", date_format("epoch", "MM")) \
  .withColumn("day", date_format("epoch", "dd")) \
  .withColumn("hour", date_format("epoch", "HH")) \
  .groupBy("thing", "year", "month", "day", "hour") \
  .agg(min("temp"), max("temp"), avg("temp"), avg("humidity"), avg("light")) \
  .sort("year", "month", "day", "hour")

display(dfEventReports)

# COMMAND ----------



# COMMAND ----------



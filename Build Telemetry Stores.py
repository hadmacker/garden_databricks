# Databricks notebook source
import os

spark.conf.set("fs.azure.account.key.gardendatabricksstorage.blob.core.windows.net", os.environ['GARDENDATA_STORAGEKEY'])

# COMMAND ----------

dfe = spark.sql("select * from events")
display(dfe)

# COMMAND ----------

from pyspark.sql.types import DateType,StructType
from pyspark.sql.functions import col, date_format, avg, min, max,input_file_name

dfEventReports = spark.sql("select * from events") \
  .withColumn("datePart", date_format("epoch", "yyyy-MM-dd")) \
  .withColumn("year", date_format("epoch", "yyyy")) \
  .withColumn("month", date_format("epoch", "MM")) \
  .withColumn("day", date_format("epoch", "dd")) \
  .withColumn("hour", date_format("epoch", "HH"))

# COMMAND ----------

display(dfEventReports)

# COMMAND ----------

dfHumidity = dfEventReports \
  .drop(col("soil")) \
  .drop(col("light")) \
  .drop(col("filename")) \
  .drop(col("temp"))

dfHumidity.write.mode("overwrite").saveAsTable("humidity")

# COMMAND ----------

dfh = spark.sql("select * from humidity")
display(dfh)

# COMMAND ----------

dfTemperatures = dfEventReports \
  .drop(col("soil")) \
  .drop(col("light")) \
  .drop(col("filename")) \
  .drop(col("humidity"))

dfTemperatures.write.mode("overwrite").saveAsTable("temperature")

# COMMAND ----------

dfLight = dfEventReports \
  .drop(col("soil")) \
  .drop(col("filename")) \
  .drop(col("humidity")) \
  .drop(col("temp"))

dfLight.write.mode("overwrite").saveAsTable("light")

# COMMAND ----------

dfl = spark.sql("select * from light")
display(dfl)

# Databricks notebook source
# MAGIC %md
# MAGIC This notebook roughly follows the content here:
# MAGIC https://docs.databricks.com/delta/optimizations/optimization-examples.html#delta-lake-on-databricks-optimizations-python-notebook

# COMMAND ----------

import os

spark.conf.set("fs.azure.account.key.gardendatabricksstorage.blob.core.windows.net", dbutils.secrets.get("gardendatabricksecrets", "gardendatabricksstorage-accesskey"))

#mount seems irrelevant for the following notebook. 
# dbutils.fs.unmount("/mnt/iotdata")
#dbutils.fs.mount(
#  source = "wasbs://reports@gardendatabricksstorage.blob.core.windows.net/data",
#  mount_point = "/mnt/iotdata",
#  extra_configs = {"fs.azure.account.key.gardendatabricksstorage.blob.core.windows.net":dbutils.secrets.get("gardendatabricksecrets", "gardendatabricksstorage-accesskey")})

# COMMAND ----------

dbutils.fs.ls("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/2021/05/06")

# COMMAND ----------

# fails with "Unable to infer schema for JSON. It must be specified manually".
# That's ok, move to the next section. But also see this SO link: https://stackoverflow.com/questions/56339089/pyspark-create-schema-from-json-schema-involving-array-columns
#df = spark.read.json("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/*")
#df.count()

# COMMAND ----------

df0505 = spark.read.json("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/2021/05/05/*")
schemadf0505 = df0505.schema.json()
print(schemadf0505)

# COMMAND ----------

import json
from pyspark.sql.types import StructType
from  pyspark.sql.functions import input_file_name

schemaJson  = '{"fields":[{"metadata":{},"name":"event","nullable":true,"type":{"fields":[{"metadata":{},"name":"metadata","nullable":true,"type":{"fields":[{"metadata":{},"name":"reported","nullable":true,"type":{"fields":[{"metadata":{},"name":"hall","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"humidity","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"light","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"message","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"soil","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"temp","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}}],"type":"struct"}}],"type":"struct"}},{"metadata":{},"name":"state","nullable":true,"type":{"fields":[{"metadata":{},"name":"reported","nullable":true,"type":{"fields":[{"metadata":{},"name":"hall","nullable":true,"type":"long"},{"metadata":{},"name":"humidity","nullable":true,"type":"long"},{"metadata":{},"name":"light","nullable":true,"type":"long"},{"metadata":{},"name":"message","nullable":true,"type":"string"},{"metadata":{},"name":"soil","nullable":true,"type":"long"},{"metadata":{},"name":"temp","nullable":true,"type":"long"}],"type":"struct"}}],"type":"struct"}},{"metadata":{},"name":"timestamp","nullable":true,"type":"long"},{"metadata":{},"name":"version","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"message","nullable":true,"type":"string"},{"metadata":{},"name":"topic","nullable":true,"type":"string"}],"type":"struct"}'

schema = StructType.fromJson(json.loads(schemaJson))

# https://docs.databricks.com/delta/optimizations/optimization-examples.html#delta-lake-on-databricks-optimizations-python-notebook
dfRaw = spark.read \
  .schema(schema) \
  .json("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/*/*/*/*") \
  .withColumn("filename", input_file_name())

dfRaw.count()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, unix_timestamp, split, date_format
from pyspark.sql import types
dfEvents = dfRaw \
  .withColumn("thing", split(col("topic"), "/", -1).getItem(2)) \
  .withColumn("timestamp", col("event.timestamp")) \
  .withColumn("epoch", col("timestamp").cast(types.TimestampType())) \
  .withColumn("humidity", col("event.state.reported.humidity")) \
  .withColumn("soil", col("event.state.reported.soil")) \
  .withColumn("light", col("event.state.reported.light")) \
  .withColumn("temp", col("event.state.reported.temp")) \
  .withColumn("version", col("event.version")) \
  .withColumn("datePart", date_format("epoch", "yyyy-MM-dd")) \
  .withColumn("year", date_format("epoch", "yyyy")) \
  .withColumn("month", date_format("epoch", "MM")) \
  .withColumn("day", date_format("epoch", "dd")) \
  .withColumn("hour", date_format("epoch", "HH")) \
  .drop(col("event")) \
  .drop(col("topic")) \
  .drop(col("message")) 

# COMMAND ----------

display(dfEvents)

# COMMAND ----------

#path = f"/mnt/iotdata/events_parquet"
path = f"wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/parquet/events_parquet"
dfEvents \
  .write \
  .format("parquet") \
  .mode("append") \
  .partitionBy("datePart") \
  .save(path)

# COMMAND ----------

# load Parquet

from pyspark.sql.functions import *

dfEventsStream = spark.read \
  .format("parquet") \
  .load("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/parquet/events_parquet")

# COMMAND ----------

display(dfEventsStream)

# COMMAND ----------

dfEventsStream \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .partitionBy("datePart") \
  .save("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/delta/events_delta")


# COMMAND ----------

dfeventsdelta = spark.read.format("delta").load("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/delta/events_delta")
display(dfeventsdelta)

# COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS events"))
display(spark.sql("CREATE TABLE events USING DELTA LOCATION 'wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/delta/events_delta'"))
display(spark.sql("OPTIMIZE events ZORDER BY (epoch)"))

# COMMAND ----------

from pyspark.sql.functions import *

dfEventsStream = spark.read \
  .format("delta") \
  .load("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/delta/events_delta")
display(dfEventsStream)

# COMMAND ----------

dfEventReports = spark.sql("select * from events")
display(dfEventReports)

# COMMAND ----------

#dbutils.fs.unmount("/mnt/iotdata")

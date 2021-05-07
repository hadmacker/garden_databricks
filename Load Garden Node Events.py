# Databricks notebook source
import os

spark.conf.set("fs.azure.account.key.gardendatabricksstorage.blob.core.windows.net", os.environ['GARDENDATA_STORAGEKEY'])

# COMMAND ----------

dbutils.fs.ls("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/2021/05/06")

# COMMAND ----------

# fails with "Unable to infer schema for JSON. It must be specified manually".
# That's ok, move to the next section. But also see this SO link: https://stackoverflow.com/questions/56339089/pyspark-create-schema-from-json-schema-involving-array-columns
df = spark.read.json("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/*")
df.count()

# COMMAND ----------

df0505 = spark.read.json("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/2021/05/05/*")
schemadf0505 = df0505.schema.json()
print(schemadf0505)

# COMMAND ----------

import json
from pyspark.sql.types import StructType

schemaJson  = '{"fields":[{"metadata":{},"name":"event","nullable":true,"type":{"fields":[{"metadata":{},"name":"metadata","nullable":true,"type":{"fields":[{"metadata":{},"name":"reported","nullable":true,"type":{"fields":[{"metadata":{},"name":"hall","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"humidity","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"light","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"message","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"soil","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"temp","nullable":true,"type":{"fields":[{"metadata":{},"name":"timestamp","nullable":true,"type":"long"}],"type":"struct"}}],"type":"struct"}}],"type":"struct"}},{"metadata":{},"name":"state","nullable":true,"type":{"fields":[{"metadata":{},"name":"reported","nullable":true,"type":{"fields":[{"metadata":{},"name":"hall","nullable":true,"type":"long"},{"metadata":{},"name":"humidity","nullable":true,"type":"long"},{"metadata":{},"name":"light","nullable":true,"type":"long"},{"metadata":{},"name":"message","nullable":true,"type":"string"},{"metadata":{},"name":"soil","nullable":true,"type":"long"},{"metadata":{},"name":"temp","nullable":true,"type":"long"}],"type":"struct"}}],"type":"struct"}},{"metadata":{},"name":"timestamp","nullable":true,"type":"long"},{"metadata":{},"name":"version","nullable":true,"type":"long"}],"type":"struct"}},{"metadata":{},"name":"message","nullable":true,"type":"string"},{"metadata":{},"name":"topic","nullable":true,"type":"string"}],"type":"struct"}'

schema = StructType.fromJson(json.loads(schemaJson))

df = spark.read.schema(schema).json("wasbs://gardennodes@gardendatabricksstorage.blob.core.windows.net/data/*/*/*/*")
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------



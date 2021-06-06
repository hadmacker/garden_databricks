# Databricks notebook source
import os

spark.conf.set("fs.azure.account.key.gardendatabricksstorage.blob.core.windows.net", dbutils.secrets.get("gardendatabricksecrets", "gardendatabricksstorage-accesskey"))

# COMMAND ----------

dfe = spark.sql("select * from events")
display(dfe)

# COMMAND ----------

dfHumidity = dfEventReports \
  .drop(col("soil")) \
  .drop(col("light")) \
  .drop(col("filename")) \
  .drop(col("temp"))

dfHumidity.write.mode("overwrite").saveAsTable("humidity")
display(spark.sql("OPTIMIZE humidity ZORDER BY (epoch)"))

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
display(spark.sql("OPTIMIZE temperature ZORDER BY (epoch)"))

# COMMAND ----------

dfLight = dfEventReports \
  .drop(col("soil")) \
  .drop(col("filename")) \
  .drop(col("humidity")) \
  .drop(col("temp"))

dfLight.write.mode("overwrite").saveAsTable("light")
display(spark.sql("OPTIMIZE light ZORDER BY (epoch)"))

# COMMAND ----------

dfl = spark.sql("select * from light")
display(dfl)

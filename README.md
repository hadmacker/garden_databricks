# Garden Node Databricks Reporting

## Objective

* Implement a Databricks solution for analysis of garden node event data.

![Architecture](design.png)

## Getting Started

* [esp32_garden_node public repository](https://github.com/hadmacker/esp32_garden_node_pub)
* [AWS CLI Configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)
  * Used to download files from garden node esp32 device.
    * AWS CLI command from .\data folder: `aws s3 sync s3://garden-nodes .`
* [Azure Storage Explorer](https://azure.microsoft.com/en-ca/features/storage-explorer/)
  * Amazing tool, easily upload nested folders of `.\data` for example.

## TODO

* [x] Create Azure Storage Account
* [x] Upload data files
* [x] Create Databricks instance
* [x] Create Databricks cluster (DS3 v2) [Cluster Pricing](https://azure.microsoft.com/en-ca/pricing/details/databricks/)
* [x] Connect Databricks instance to Azure Storage Account [](https://caiomsouza.medium.com/how-to-connect-azure-databricks-with-azure-blob-storage-1b3307620524)
  * Added environment variable `GARDENDATA_STORAGEKEY` with Azure Storage Account Key. [This link](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage) has other methods to connect Azure Blob Storage.
* [x] Job to injest data into Databricks Bronze Layer
* [x] Job to injest data into Databricks Silver Layer
* [x] Job to injest data into Databricks Gold Layer
* [x] Secrets management (using `dbutils.secrets`, not `os.environ`)
* [ ] Join streams with Govt. of Canada Weather/precipitation data
  * Data: [GOC, Daily Data Report for May 2021](https://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174)

## Databricks Documentation
* [SQL Reference](https://spark.apache.org/docs/3.1.1/sql-ref.html)
* Python API (PySpark)
  * Main module: `pyspark.sql.functions`
    * blanket import (not recommended): `from pyspark.sql.functions import *`
  * [PySpark (Latest)](https://spark.apache.org/docs/3.1.1/api/python/reference/index.html)
  * [PySpark (3.1.1)](https://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.sql.html)
  * [PySpark (3.0.1)](https://spark.apache.org/docs/3.0.1/api/python/pyspark.sql.html#module-pyspark.sql)
    * [Data Types](https://spark.apache.org/docs/3.0.1/sql-ref-datatypes.html)
      * module: `pyspark.sql.types`
    * [DateTime Patterns](https://spark.apache.org/docs/3.0.1/sql-ref-datetime-pattern.html)
* [Apache Spark 3.1.1 Documentation:](https://spark.apache.org/docs/3.1.1/)
  * [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
    * writeStream ^^
    * [Structured Streaming to Event Hubs with PySpark](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md)
* [Databricks Delta Engine guide](https://docs.databricks.com/delta/)
* [Azure Databricks Documentation](https://docs.microsoft.com/en-us/azure/databricks/)
* [Microsoft: Table streaming reads and writes](https://docs.microsoft.com/en-us/azure/databricks/delta/delta-streaming)
* [Microsoft: SQL reference for SQL Analytics](https://docs.microsoft.com/en-us/azure/databricks/sql/language-manual)
* [Microsoft: SQL Analytics Quickstart: Run and visualize a query](https://docs.microsoft.com/en-us/azure/databricks/sql/get-started/user-quickstart)
* [Azure Databricks Pricing](https://azure.microsoft.com/en-ca/pricing/details/databricks/)
  * DS3 v2 4 vCPU 14 GiB RAM 0.75 DBU $0.743/hour CAD
* [Databricks Visualizations](https://docs.databricks.com/notebooks/visualizations/index.html)
* Secrets Management
  1. [Secret Scopes](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes)
    1.1. Create Azure Key Vault
    1.2. Get Key Vault DNS URL + ResourceID path
    1.3. Come up with a name for your Secrets Scope. Remember this, write it down somewhere. (Looking this up later is not fun.)
    1.4. Add 1.2 items here: `https://<databricks-instance>#secrets/createScope`
  2. [Secrets](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secrets)
    2.1. Create secret in Azure Key Vault
  3. [Secrets utilities](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils#dbutils-secrets)
    3.1. Syntax: `dbutils.secrets.get("secretscopename", "secretname")`
  4. [Databricks CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/)
    4.1. Did you skip step 1.3? Yeah... now you'll need the link above. ^^

## Databricks Notes

* Linking to Azure Storage Account
  * https://caiomsouza.medium.com/how-to-connect-azure-databricks-with-azure-blob-storage-1b3307620524
    * Add Storage Account Access Key to Databricks cluster environment variables.
  * https://www.sqlshack.com/accessing-azure-blob-storage-from-azure-databricks/
  * https://forums.databricks.com/questions/25088/read-json-file-to-azure-sql.html
* Cluster Libraries
  * com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18
  * com.databricks:spark-xml_2.12:0.11.0
* [Reading JSON files in Databricks](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/read-json)
* [ls recursive databricks](https://stackoverflow.com/questions/63955823/list-the-files-of-a-directory-and-subdirectory-recursively-in-databricksdbfs)
* [Azure Sources and Sinks](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/data-sources)
  * [Azure Event Hubs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs)
* [Video: Simplify and Scale Data Engineering Pipelines with Delta Lake](https://databricks.com/session_eu19/simplify-and-scale-data-engineering-pipelines-with-delta-lake)
* [How to connect databricks with Azure Blob Storage](https://caiomsouza.medium.com/how-to-connect-azure-databricks-with-azure-blob-storage-1b3307620524)
* [SO: Unable to infer schema](https://stackoverflow.com/questions/56339089/pyspark-create-schema-from-json-schema-involving-array-columns)
* [Layers: Bronze, Silver, Gold](https://databricks.com/blog/2019/08/14/productionizing-machine-learning-with-delta-lake.html)
  * Bronze: Raw Injestion
  * Silver: Filtered, cleaned, augmented
  * Gold: Business-level aggregates & reporting
* Secrets Management
  * Blog: [Visualize data with Azure Databricks](https://medium.com/analytics-vidhya/visualize-data-in-azure-databricks-d9f087be093d)
    * Looks like a good E2E Databricks walkthrough, includes proper secrets management. Have not yet tried this.
  * [Mount a Blob container](https://docs.databricks.com/data/data-sources/azure/azure-storage.html#azure-blob-storage-notebook)
    * Top command includes reference to secrets for key.

## Libraries

### Boto3 (Python, AWS)

* [Downloading Files, Amazon S3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html)

### com.microsoft.azure:azure-eventhubs-spark

* [Azure Event Hubs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs)
  * [github guide](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md)
* [Maven Repository](https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark)

## Questions

* Q: Why are you using both AWS and Azure in this solution?
  * A: I used to work with AWS. I now work with Azure. It's easier for me to get invoice credits in Azure than AWS currently so that's where I'm continuing the project.

# Pricing

* [Azure Event Hubs Pricing](https://azure.microsoft.com/en-ca/pricing/details/event-hubs/)
  * Capture, if needed, requires Standard SKU.
* [Azure Databricks Pricing](https://azure.microsoft.com/en-us/pricing/details/databricks/)
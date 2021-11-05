from delta import *
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

storage_account = os.environ.get("STORAGE_ACCOUNT")
storage_key = os.environ.get("STORAGE_KEY")

builder = SparkSession.builder.appName("exampleApp") \
    .config("spark.jars.packages","io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-azure:3.2.1,com.microsoft.azure:azure-storage:8.6.3")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",storage_key)

spark = builder.getOrCreate()

data = spark.range(0, 5)

data.show()
data.write.format("delta").mode("overwrite").save(f"abfss://datalake@{storage_account}.dfs.core.windows.net/test")



def set_azure_credentials(spark: SparkSession):
    spark._jsc.hadoopConfiguration().set(
        f"fs.azure.account.auth.type.{config['storage_account']}.dfs.core.windows.net",
        "SharedKey",
    )
    spark._jsc.hadoopConfiguration().set(
        f"fs.azure.account.key.{config['storage_account']}.dfs.core.windows.net",
        config["storage_account_key"],
    )
    spark._jsc.hadoopConfiguration().set(
        "fs.defaultFS",
        f"abfs://{config['storage_container']}@{config['storage_account']}.dfs.core.windows.net/",
    )

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os
from src.utils import PurviewPOCClient
from pyapacheatlas.auth import ServicePrincipalAuthentication

load_dotenv()

storage_account = os.environ.get("STORAGE_ACCOUNT")
storage_key = os.environ.get("STORAGE_KEY")
scan = os.environ.get("SCAN")
tenant_id = os.environ.get("TENANT_ID")
client_id = os.environ.get("CLIENT_ID")
client_secret = os.environ.get("CLIENT_SECRET")
account_name = os.environ.get("PURVIEW_ACCOUNT")

jars = (
    ""
    + "io.delta:delta-core_2.12:1.0.0,"
    + "org.apache.hadoop:hadoop-azure:3.2.1,"
    + "com.microsoft.azure:azure-storage:8.6.3"
)


builder = (
    SparkSession.builder.appName("exampleApp")
    .config("spark.jars.packages", jars)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
)

spark = builder.getOrCreate()

country_data = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .format("csv")
    .load("./data/country_data.csv")
    .select(
        [
            "Country",
            "Region",
            "Surface area (km2)",
            "Population in thousands (2017)",
            "Sex ratio (m per 100 f, 2017)",
            "GDP: Gross domestic product (million current US$)",
        ]
    )
    .withColumnRenamed("Surface area (km2)", "Surface_area")
    .withColumnRenamed("Population in thousands (2017)", "Population_in_thousands")
    .withColumnRenamed("Sex ratio (m per 100 f, 2017)", "Sex_ratio")
    .withColumnRenamed("GDP: Gross domestic product (million current US$)", "GDP")
    .withColumn("Surface_area", col("Surface_area").cast("double"))
)

vaccine_data = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .format("csv")
    .load("./data/vaccine_data.csv")
    .withColumnRenamed(
        "Doses administered per 100 people", "Doses_administered_per_100_people"
    )
    .withColumnRenamed("Total doses administered", "Total_doses_administered")
    .withColumnRenamed("% of population vaccinated", "Percentage_population_vaccinated")
    .withColumnRenamed(
        "% of population fully vaccinated", "Percentage_population_fully_vaccinated"
    )
)

combined_data = vaccine_data.join(other=country_data, on="Country")

(
    combined_data.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(
        f"abfss://datalake@{storage_account}.dfs.core.windows.net/clean/extended_vaccine_data"
    )
)

(
    combined_data.coalesce(1)
    .write.format("parquet")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(
        f"abfss://datalake@{storage_account}.dfs.core.windows.net/master/extended_vaccine_data"
    )
)

auth = ServicePrincipalAuthentication(
    tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
)
client = PurviewPOCClient(account_name=account_name, authentication=auth)

if int(scan) == 1:
    print("hello")
    client.create_or_update_collection(collection_name="MyCollection")
    client.register_Adls2_data_source(
        source_name="MyAdls2Source",
        storage_account=storage_account,
        collection_name="MyCollection",
    )
    client.create_or_update_scan(
        source_name="MyAdls2Source",
        scan_name="MyCustomScan",
        collection_name="MyCollection",
    )
    client.run_scan(
        source_name="MyAdls2Source", scan_name="MyCustomScan", scan_level="Incremental"
    )

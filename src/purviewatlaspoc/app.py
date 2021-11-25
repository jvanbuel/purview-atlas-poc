from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os
from purviewatlaspoc.common.utils import PurviewPOCClient
from purviewatlaspoc.common.spark import ClosableSparkSession

load_dotenv()

storage_account = os.environ.get("STORAGE_ACCOUNT")
storage_key = os.environ.get("STORAGE_KEY")
scan = os.environ.get("SCAN")


class PurviewDemoPipeline:
    def __init__(self, spark: SparkSession, purview_client: PurviewPOCClient):
        self.spark = spark
        self.base_storage_path = (
            f"abfss://datalake@{storage_account}.dfs.core.windows.net"
        )

    def extract(self):
        vaccine_data = (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .format("csv")
            .load("./data/vaccine_data.csv")
        )
        country_data = (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .format("csv")
            .load("./data/country_data.csv")
        )
        return vaccine_data, country_data

    def transform(self, vaccine_data: DataFrame, country_data: DataFrame):
        clean_country_data = (
            country_data.select(
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
            .withColumnRenamed(
                "Population in thousands (2017)", "Population_in_thousands"
            )
            .withColumnRenamed("Sex ratio (m per 100 f, 2017)", "Sex_ratio")
            .withColumnRenamed(
                "GDP: Gross domestic product (million current US$)", "GDP"
            )
            .withColumn("Surface_area", col("Surface_area").cast("double"))
        )

        clean_vaccine_data = (
            vaccine_data.withColumnRenamed(
                "Doses administered per 100 people", "Doses_administered_per_100_people"
            )
            .withColumnRenamed("Total doses administered", "Total_doses_administered")
            .withColumnRenamed(
                "% of population vaccinated", "Percentage_population_vaccinated"
            )
            .withColumnRenamed(
                "% of population fully vaccinated",
                "Percentage_population_fully_vaccinated",
            )
        )

        extended_vaccine_data = clean_vaccine_data.join(
            other=clean_country_data, on="Country"
        )

        return extended_vaccine_data

    def write(
        self,
        extended_vaccine_data: DataFrame,
    ):
        (
            extended_vaccine_data.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{self.base_storage_path}/clean/extended_vaccine_data")
        )

        (
            extended_vaccine_data.coalesce(1)
            .write.format("parquet")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{self.base_storage_path}/master/extended_vaccine_data")
        )
        return

    def run(self):
        self.write(self.transform(*self.extract()))


if __name__ == "__main__":

    with ClosableSparkSession(
        app_name="purviewdemo",
        spark_config={
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net": storage_key
        },
    ) as spark:
        purview_client = PurviewPOCClient()
        pipeline = PurviewDemoPipeline(spark=spark, purview_client=purview_client)
        pipeline.run()

        if int(scan) == 1:

            purview_client.create_or_update_collection(collection_name="MyCollection")
            purview_client.register_Adls2_data_source(
                source_name="MyAdls2Source",
                storage_account=storage_account,
                collection_name="MyCollection",
            )
            purview_client.create_or_update_scan(
                source_name="MyAdls2Source",
                scan_name="MyCustomScan",
                collection_name="MyCollection",
            )
            purview_client.run_scan(
                source_name="MyAdls2Source",
                scan_name="MyCustomScan",
                scan_level="Incremental",
            )

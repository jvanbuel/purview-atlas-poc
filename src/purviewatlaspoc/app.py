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
    def __init__(self, spark: ClosableSparkSession, purview_client: PurviewPOCClient):
        self.spark = spark
        self.base_storage_path = (
            f"abfss://datalake@{storage_account}.dfs.core.windows.net"
        )
        self.purview_client = purview_client

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

        return extended_vaccine_data, clean_vaccine_data, clean_country_data

    def write(
        self,
        extended_vaccine_data: DataFrame,
        clean_vaccine_data: DataFrame,
        clean_country_data: DataFrame,
    ):

        # write to clean 
        for df, name in (
            (extended_vaccine_data, "extended_vaccine"),
            (clean_country_data, "clean_country"),
            (clean_vaccine_data, "clean_vaccine"),
        ):
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(f"{self.base_storage_path}/clean/{name}_data")
            )
            # register individual delta tables
            self.purview_client.register_delta_table(df=df, name=name)

        # register lineage
        self.purview_client.register_delta_lineage(
            name="purviewdemo",
            inputs=["clean_country", "clean_vaccine"],
            outputs=["extended_vaccine"],
        )

        # write to master 
        (
            extended_vaccine_data.coalesce(1)
            .write.format("parquet")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{self.base_storage_path}/master/extended_vaccine_data")
        )

        # optional scanning of ADLS
        if int(scan) == 1:
            self.purview_client.scan()

        return

    def run(self):
        self.write(*self.transform(*self.extract()))


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

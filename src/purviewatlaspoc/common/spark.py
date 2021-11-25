import logging

from pyspark.sql import SparkSession


class SparkLogger:
    def __init__(self, spark: SparkSession):
        conf = spark.sparkContext.getConf()
        app_id = conf.get("spark.app.id")
        app_name = conf.get("spark.app.name")

        log4j = spark._jvm.org.apache.log4j
        message_prefix = "<" + app_name + " " + app_id + ">"
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log an warning.
        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None


class ClosableSparkSession:
    def __init__(
        self,
        app_name: str,
        master: str = None,
        spark_config: dict = {},
    ):
        self._app_name = app_name
        self._master = master
        self._spark_config = spark_config
        self._spark_session = None

    def __enter__(self):
        spark_builder = SparkSession.builder.appName(self._app_name)

        # set master if needed
        if self._master:
            spark_builder = spark_builder.master(self._master)

        # set some default configuration
        jars = (
            ""
            + "io.delta:delta-core_2.12:1.0.0,"
            + "org.apache.hadoop:hadoop-azure:3.2.1,"
            + "com.microsoft.azure:azure-storage:8.6.3"
        )
        spark_builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark_builder.config("spark.jars.packages", jars)
        spark_builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        spark_builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 

        # add other config params
        for key, val in self._spark_config.items():
            spark_builder.config(key, val)

        # create the actual session

        self._spark_session = spark_builder.getOrCreate()

        return self._spark_session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            logging.error(exc_tb)
        if self._spark_session:
            self._spark_session.stop()

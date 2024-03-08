from pyspark.sql import SparkSession
from delta import *


class SparkSessionBuilder:
    """
    This class is responsible for giving a valid spark session
    """

    @classmethod
    def get_session(cls) -> SparkSession:
        """
        A spark session to use for local testing
        """
        builder = SparkSession.builder \
            .appName("pybdd-test-application") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

from pyspark.sql import SparkSession

class SparkSessionBuilder:
    """
    This class is responsible for giving a valid spark session
    """

    @classmethod
    def get_session(cls) -> SparkSession:
        """
        A spark session to use for local testing
        """
        spark = SparkSession.builder \
            .appName("pybdd-test-application") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()

        return spark

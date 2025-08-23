from pyspark .sql import SparkSession


def get_spark_seesion() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Spark session is not active. Please start a Spark session before using this function.")
    return spark

# import pyspark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


class SharedParametersTools(object):
    def __init__(self, use_spark: bool = False):
        if use_spark:
            self.spark = self.create_spark_session()

    @staticmethod
    def create_spark_session() -> SparkSession:
        app_name = "sisi ops test"

        builder = (
            SparkSession.builder
                        .appName(app_name)
                        .master("local[4]")
                        .enableHiveSupport()
                        .config(
                            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                        )
                        .config(
                            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                        )
                        .config("spark.speculation", "true")
                        .config("spark.sql.datetime.java8API.enabled", "true")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.catalog.setCurrentCatalog("spark_catalog")
        spark.sparkContext.setLogLevel("WARN")
        return spark

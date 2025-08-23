import unittest
from sisi_ops.utils.helper.testing_tools import SharedParameters
from pyspark.sql import SparkSession

APP_NAME = "sisi ops test"
DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
MASTER = "local[4]"
LOG_LEVEL = "WARN"


class TestCreateSparkSession(unittest.TestCase):
    def test_create_spark_session(self):
        spark = SharedParameters.create_spark_session()
        self.assertIsNotNone(spark)
        self.assertIsInstance(spark, SparkSession)
        self.assertIn(DELTA_EXTENSION, spark.conf.get("spark.sql.extensions"))
        self.assertEqual(spark.conf.get("spark.sql.catalog.spark_catalog"), DELTA_CATALOG)
        self.assertEqual(spark.sparkContext.appName, f"{APP_NAME}")
        self.assertTrue(spark.sparkContext.master.startswith(f"local"))
        # Use JVM bridge to get log level
        log4j = spark._jvm.org.apache.log4j
        logger = log4j.LogManager.getRootLogger()
        self.assertEqual(logger.getLevel().toString(), f"{LOG_LEVEL}")
        spark.stop()

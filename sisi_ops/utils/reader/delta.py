from pyspark.sql import SparkSession, DataFrame

from .base_reader import BaseReader

class DeltaReader(BaseReader):
    def __init__(self, spark: SparkSession) -> None:
        super().__init__()
        if isinstance(spark, SparkSession):
            self.spark = spark
        else:
            raise TypeError(
                f"Parameter: Spark is not SparkSeesion instance, currently is {type(spark)}."
            )
    
    def read_table(self, table_path) -> DataFrame:
        return self.spark.read.table(table_path)
    
    def read_file(self, file_path) -> DataFrame:
        file_format = file_path.split(".")[-1]
        return self.spark.read.format(file_format).option("header", "true").load(file_path)

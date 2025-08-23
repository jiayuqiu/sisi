from pyspark.sql import SparkSession, DataFrame

from ..base_reader import BaseReader

class DeltaReader(BaseReader):
    def __init__(self, spark: SparkSession) -> None:
        super().__init__()
        if isinstance(spark, SparkSession):
            self.spark = spark
        else:
            raise TypeError(
                "SparkSession is  not defined. SparkSession is NECESSARY for DeltaReader"
            )
    
    def read_table(self, table_path) -> DataFrame:
        return self.spark.read.table(table_path)
    
    def read_file(self, file_path) -> DataFrame:
        file_format = file_path.split(".")[-1]
        return self.spark.read.format(file_format).load(file_path)

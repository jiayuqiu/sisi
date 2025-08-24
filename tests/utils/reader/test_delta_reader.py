# -*- encoding: utf-8 -*-
"""
@File    :   test_reader.py
@Time    :   2025/08/23 13:06:29
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   unit test for delta reader module
"""

import unittest

from pyspark.sql import SparkSession, DataFrame
from sisi_ops.utils.reader.delta import DeltaReader
from sisi_ops.utils.helper.testing_tools import SharedParametersTools


class TestDeltaReader(unittest.TestCase):

    def setUp(self):
        self.spt = SharedParametersTools(use_spark=True)

    def test_read_file_calls_spark(self):
        # initialize delta reader
        reader = DeltaReader(self.spt.spark)

        # load by reader
        expected_columns = ['mmsi', 'acqtime', 'cog', 'latitude', 'longitude', 'status', 'true_head', 'avg_lon', 'avg_lat']
        expect_data_size = 10000
        df = reader.read_file("data/dummy/ais/202301.csv")
        
        self.assertEqual(df.count(), expect_data_size)
        self.assertEqual(list(df.columns), expected_columns)
    
    def test_invalid_spark(self):
        with self.assertRaises(TypeError):
            DeltaReader(spark=123)

    # def test_read_file_calls_spark(self):
    #     self.reader.read_file("data.parquet")
    #     self.mock_spark.read.format.assert_called_once_with("parquet")

    # def test_invalid_spark(self):
    #     with self.assertRaises(TypeError):
    #         DeltaReader(spark="not_a_spark")


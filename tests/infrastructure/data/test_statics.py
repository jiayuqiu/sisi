"""
@File    :   test_statics.py
@Time    :   2025/02/23 13:29:38
@Author  :   jerry
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   unit test for statics data
"""

import os
import unittest
from dotenv import load_dotenv

from core.infrastructure.data.statics import StaticsDataProcessor
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm


class TestEvents(unittest.TestCase):
    load_dotenv(".env")
    stage_env = os.environ["TEST_STAGE_ENV"]
    month_str = "202311"
    vars = Vm(stage_env)

    def setUp(self):
        """Run some functions at beginning. Nothing to do yet."""
        sdp = StaticsDataProcessor(
            csv_file=os.path.join(
                self.vars.dp_names.data_path,
                self.stage_env,
                'statics',
                f"{self.month_str}.csv"
            )
        )
        self.wrangled_df = sdp.wrangle()

    def tearDown(self):
        """Run some functions at end of the testing. Nothing to do yet."""

    def test_statics_date_id(self, ):
        """
        inspect statics data if each date_id and mmsi only contains one static record.
        """
        # Check for duplicates in the combination of date_id and mmsi columns.
        duplicates = self.wrangled_df.duplicated(subset=['date_id', 'mmsi'])
        self.assertFalse(duplicates.any(),
                         "Duplicate statics record found for the same date_id and mmsi combination")

    def test_statics_wrangle(self, ):
        """test for statics data wrangling"""
        # Valid length_width_ratio
        # 1. Check that the length_width_ratio column contains no NaN values
        self.assertFalse(self.wrangled_df['length_width_ratio'].isna().any(),
                         "NaN found in length_width_ratio column")

        # 2. Check that length and width column contains no NaN values
        self.assertFalse(self.wrangled_df['length'].isna().any(),
                         "NaN found in length column")
        self.assertFalse(self.wrangled_df['width'].isna().any(),
                         "NaN found in width column")

        # 3. Check that mmsi column are all Integer
        self.assertTrue(self.wrangled_df['mmsi'].dtype.kind in 'iu',
                        "mmsi column is not of integer type")

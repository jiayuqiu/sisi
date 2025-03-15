"""
@File    :   test_statics.py
@Time    :   2025/02/23 13:29:38
@Author  :   jerry
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   unit test for statics data
"""


import unittest

from core.infrastructure.data.statics import StaticsDataProcessor
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm

class TestEvents(unittest.TestCase):
    stage_env = "test"
    vars = Vm(stage_env)

    def setUp(self):
        """Run some functions at beginning. Nothing to do yet."""

    def tearDown(self):
        """Run some functions at end of the testing. Nothing to do yet."""

    def test_statics_wrangle(self, ):
        """test for statics data clean up"""
        sdp = StaticsDataProcessor(csv_path="data/dev/statics/static_202311.csv")
        wrangled_df = sdp.wrangle()

        # valid length_width_ratio
        # Check that the length_width_ratio column contains no NaN values
        self.assertFalse(wrangled_df['length_width_ratio'].isna().any(),
                         "NaN found in length_width_ratio column")

"""
@File    :   test_statics.py
@Time    :   2025/02/23 13:29:38
@Author  :   jerry
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   unit test for statics data
"""


import unittest

import pandas as pd

from core.infrastructure.data.statics import clean_up, load_statics_data
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm

class TestEvents(unittest.TestCase):
    stage_env = "test"
    vars = Vm(stage_env)

    def setUp(self):
        """Run some functions at beginning. Nothing to do yet."""

    def tearDown(self):
        """Run some functions at end of the testing. Nothing to do yet."""

    def test_statics_clean_up(self, ):
        """test for statics data clean up"""
        df = load_statics_data("data/dev/statics/static_202311.csv")
        cleaned_df = clean_up(df)
        print("Done.")
"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  test_analysis.py
@DateTime:  16/11/2024 9:03 pm
@DESC    :  unittest for analysis
"""

import os
from dotenv import load_dotenv
import unittest

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager

class TestAnalysis(unittest.TestCase):
    load_dotenv(".env")
    stage_env = os.environ["TEST_STAGE_ENV"]  # TODO: deduplicate stage_env loading code.
    var = ShoreNetVariablesManager(stage_env)

    @unittest.skip("skip test polgyon score has been deprecated.")
    def test_polygon_score(self):
        """
        Deprecated:
        this function tests polygon score

        :return:
        """
        self.assertEqual(1, 1)

"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  test_analysis.py
@DateTime:  16/11/2024 9:03 pm
@DESC    :  unittest for analysis
"""


import unittest

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager

class TestAnalysis(unittest.TestCase):
    stage_env = "dummy"
    var = ShoreNetVariablesManager(stage_env)

    @unittest.skip("skip test")
    def test_polygon_score(self):
        """
        Deprecated:
        this function tests polygon score

        :return:
        """
        self.assertEqual(1, 1)

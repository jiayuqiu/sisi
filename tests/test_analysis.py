"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  test_analysis.py
@DateTime:  16/11/2024 9:03 pm
@DESC    :  unittest for analysis
"""


import unittest

from core.ShoreNet.definitions.variables import VariablesManager

class TestAnalysis(unittest.TestCase):
    var = VariablesManager()

    def test_polygon_score(self):
        """
        this function tests polygon score

        :return:
        """
        from core.ShoreNet.analyze.polygon.trust_score import DockPolygon
        self.assertEqual(1, 1)

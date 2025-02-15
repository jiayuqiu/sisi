# -*- encoding: utf-8 -*-
'''
@File    :   test_polygon_parse.py
@Time    :   2025/02/15 11:42:54
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   This script is to test the polygon parse functions
'''


import os
import unittest

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.utils.polygon import KMLParser, CoordinatePoint


class TestEvents(unittest.TestCase):
    stage_env = "dummy"
    vars = VariablesManager(stage_env)
    
    def test_kml_parse(self):
        """load kml file and parse it into polygon data"""
        kml_file = os.path.join(
            self.vars.dp_names.test_utils_data_path,
            'kml', 'qhd.kml'
        )
        kp = KMLParser(kml_file, self.vars).parse_kml()[0]

        # mock data
        mock_points = [[123.123, 23.23], [124.124, 24.24], [125.125, 25.25], [126.126, 26.26], [123.123, 23.23]]
        
        for mock_point, parsed_point in zip(mock_points, kp["polygon"]):
            self.assertEqual(CoordinatePoint(mock_point[1], mock_point[0]), parsed_point)

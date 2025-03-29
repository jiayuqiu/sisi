# -*- encoding: utf-8 -*-
'''
@File    :   test_geo.py
@Time    :   2025/02/22 01:28:02
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

import os
import unittest
from dotenv import load_dotenv

import numpy as np

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.ShoreNet.utils.geo import point_poly, point_poly_batch
from core.cython.geo_cython import point_poly_c


def point_poly_general_batch(points, polygon, inspect_method):
    n_points = points.shape[0]
    results = np.empty(n_points, dtype=np.bool_)
    for i in range(n_points):
        lng = points[i, 0]
        lat = points[i, 1]
        results[i] = inspect_method(lng, lat, polygon)
    return results


class TestEvents(unittest.TestCase):
    load_dotenv(".env")
    stage_env = os.environ["TEST_STAGE_ENV"]
    vars = ShoreNetVariablesManager(stage_env)
    random_points = np.random.uniform(low=0.0, high=20.0, size=(10000000, 2))
    polygon_1 =  [[1.0, 1.0], [1.0, 9.0], [9.0, 9.0], [9.0, 1.0]]
    polygon_np_1 = np.array(polygon_1, dtype=np.float64)

    def test_c_point_polygon(self):  # 7.5
        point_poly_general_batch(self.random_points, self.polygon_np_1, point_poly_c)

    def test_point_polygon(self):  
        point_poly_general_batch(self.random_points, self.polygon_np_1, point_poly)
    
    def test_numba_point_polygon(self):
        # Process all points in parallel
        point_poly_batch(self.random_points, self.polygon_np_1)

    def test_compare_point_polygon(self):
        """Generate 10M random points and compare results from both implementations."""
        polygon = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        random_points = np.random.uniform(low=0.0, high=10.0, size=(10000000, 2))
        for lng, lat in random_points:
            res_c = point_poly_c(lng, lat, polygon)
            res_py = point_poly(lng, lat, polygon)
            self.assertEqual(res_c, res_py, f"Mismatch for point ({lng}, {lat})")
# -*- encoding: utf-8 -*-
'''
@File    :   test_geo.py
@Time    :   2025/02/22 01:28:02
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

import time
import unittest
from functools import wraps

import numpy as np

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.ShoreNet.utils.geo import point_poly
from core.cython.geo_cython import point_poly_c


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"{func.__name__} executed in {end - start:.6f} seconds")
        return result
    return wrapper


class TestEvents(unittest.TestCase):
    stage_env = "dummy"
    vars = ShoreNetVariablesManager(stage_env)

    def test_c_point_polygon(self):  # 7.5
        polygon = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        for i in range(10000000):
            point_poly_c(2.0, 3.0, polygon)

    def test_point_polygon(self):  
        polygon = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        for i in range(10000000):  # 44
            point_poly(2.0, 3.0, polygon)

    def test_compare_point_polygon(self):
        """Generate 10M random points and compare results from both implementations."""
        polygon = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        random_points = np.random.uniform(low=0.0, high=10.0, size=(10000000, 2))
        for lng, lat in random_points:
            res_c = point_poly_c(lng, lat, polygon)
            res_py = point_poly(lng, lat, polygon)
            self.assertEqual(res_c, res_py, f"Mismatch for point ({lng}, {lat})")
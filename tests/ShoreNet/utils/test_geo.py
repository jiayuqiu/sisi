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

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager
from sisi_ops.ShoreNet.utils.geo import point_poly, point_poly_batch
from sisi_ops.cython.geo_cython import point_poly_c


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
    random_points = np.random.uniform(low=0.0, high=20.0, size=(15000000, 2))
    polygon_1 =  [[1.0, 1.0], [1.0, 9.0], [9.0, 9.0], [9.0, 1.0]]
    polygon_np_1 = np.array(polygon_1, dtype=np.float64)

    def test_c_point_polygon(self):
        results = point_poly_general_batch(self.random_points, self.polygon_np_1, point_poly_c)
        print(results[results==True])

    # @unittest.skip("takes too long - about 8 - 10 times to point_poly_c")
    def test_point_polygon(self):
        results = point_poly_general_batch(self.random_points, self.polygon_np_1, point_poly)
        print(results[results==True])
    
    def test_numba_point_polygon(self):
        results = point_poly_batch(self.random_points, self.polygon_np_1)
        print(results[results==True])

    def test_compare_point_polygon(self):
        """Compare results from C, Python, and Numba implementations."""
        polygon = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        polygon_np = np.array(polygon)
        num_points = 1000000  # Reduced for faster testing
        random_points = np.random.uniform(low=0.0, high=10.0, size=(num_points, 2))

        # Calculate results using all three implementations
        results_c = [point_poly_c(lng, lat, polygon) for lng, lat in random_points]
        results_py = [point_poly(lng, lat, polygon) for lng, lat in random_points]
        results_numba = point_poly_batch(random_points, polygon_np)

        # Compare the results
        for i in range(num_points):
            lng, lat = random_points[i]
            self.assertEqual(results_c[i], results_py[i], f"C vs Python mismatch for point ({lng}, {lat})")
            self.assertEqual(results_c[i], results_numba[i], f"C vs Numba mismatch for point ({lng}, {lat})")

    def test_numba_point_polygon_on_ais(self):
        import pandas as pd

        ais_df = pd.read_csv(
            os.path.join(self.vars.dp_names.data_path, "dummy/ais/202301.csv")
        )

        ais_df.loc[:, "longitude"] = ais_df.loc[:, "longitude"] / 1e6
        ais_df.loc[:, "latitude"] = ais_df.loc[:, "latitude"] / 1e6

        polygon = [
            [121.7, 29.0],  # bottom-left
            [121.7, 29.3],  # top-left
            [122.0, 29.3],  # top-right
            [122.0, 29.0]  # bottom-right
        ]

        results: np.ndarray = point_poly_batch(np.array(ais_df.loc[:, ["longitude", "latitude"]]),
                                               np.array(polygon, dtype=np.float64))

        # count true and false results value
        self.assertEqual(len(results[results == True]), 1178)
        self.assertEqual(len(results[results == False]), 8822)

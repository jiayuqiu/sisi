"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  test_events.py
@DateTime:  10/11/2024 2:49 pm
@DESC    :  unit tests for events
"""

import os
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.analyze.departure_destination_docks import map_dock_pairs


class TestEvents(unittest.TestCase):
    var = VariablesManager()
 
    def test_dock_pairs(self):
        """
        tests dock pairs

        :return:
        """
        # -. load tests data
        df = pd.read_csv(
            os.path.join(
                self.var.root_path,
                self.var.dp_names.test_analyze_source_data_path,
                'test_departure_destination_docks.csv'
            )
        )
        cal_dd_df = map_dock_pairs(df)
        res_dd_df = pd.read_csv(
            os.path.join(
                self.var.root_path,
                self.var.dp_names.test_analyze_result_data_path,
                'test_departure_destination_docks.csv'
            )
        )
        assert_frame_equal(cal_dd_df, res_dd_df)

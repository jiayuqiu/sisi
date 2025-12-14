"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  test_events.py
@DateTime:  10/11/2024 2:49 pm
@DESC    :  unit tests for events
"""

import os
import unittest
from dotenv import load_dotenv
import pandas as pd
from pandas.testing import assert_frame_equal

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager
from sisi_ops.ShoreNet.analyze.departure_arrival_docks import map_dock_pairs


class TestEvents(unittest.TestCase):
    load_dotenv(".env")
    stage_env = os.environ["TEST_STAGE_ENV"]  # TODO: deduplicate stage_env loading code.
    vars = ShoreNetVariablesManager(stage_env)
 
    def test_dock_pairs(self):
        """
        tests dock pairs

        :return:
        """
        # -. load tests data
        df = pd.read_csv(
            os.path.join(
                self.vars.dp_names.test_analyze_source_data_path,
                'test_departure_arrival_docks.csv'
            ),
            low_memory=True
        )
        cal_dd_df = map_dock_pairs(df).loc[:, 
                                           ["mmsi", 
                                            "departure_dock_id", "departure_lng", "departure_lat", 
                                            "arrival_dock_id", "arrival_lng", "arrival_lat", 
                                            "sail_duration"]]
        res_dd_df = pd.read_csv(
            os.path.join(
                self.vars.dp_names.test_analyze_result_data_path,
                'test_departure_arrival_docks.csv'
            ),
            low_memory=True
        )
        assert_frame_equal(cal_dd_df, res_dd_df)

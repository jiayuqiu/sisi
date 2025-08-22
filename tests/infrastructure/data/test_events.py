# -*- encoding: utf-8 -*-
"""
@File    :   test_events.py
@Time    :   2025/03/02 15:07:25
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
"""

import os
import unittest
from dotenv import load_dotenv
import pandas as pd

from sisi_ops.infrastructure.data.events import EventsDataProcessor
from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm

class TestEvents(unittest.TestCase):
    load_dotenv(".env")
    stage_env = os.environ["TEST_STAGE_ENV"]
    month_str = "202311"
    year = 2023
    month = 11
    vars = Vm(stage_env)

    def setUp(self):
        """Run some functions at beginning. Nothing to do yet."""
        sdp = EventsDataProcessor(
            csv_file=os.path.join(
                self.vars.dp_names.data_path,
                self.stage_env,
                'events',
                f"{self.month_str}.csv"
            )
        )
        self.wrangled_df = sdp.wrangle(year=self.year, month=self.month)
    
    def tearDown(self):
        """Run some functions at end of the testing. Nothing to do yet."""
    
    def test_coordinate_range(self):
        """Step 1 & 4: Check if longitude and latitude are in [-180, 180] and [-90, 90] respectively."""
        df = self.wrangled_df
        self.assertTrue(
            df['begin_lng'].between(-180, 180).all(),
            "Not all begin_lng values are within [-180, 180]."
        )
        self.assertTrue(
            df['begin_lat'].between(-90, 90).all(),
            "Not all begin_lat values are within [-90, 90]."
        )

    def test_filter_events_by_params(self):
        """Step 2: Verify events are filtered based on vars.event_param ranges and avg_speed."""
        df = self.wrangled_df
        lng_lower, lng_upper = self.vars.event_param.event_lng_range
        lat_lower, lat_upper = self.vars.event_param.event_lat_range
        self.assertTrue(
            df['begin_lng'].gt(lng_lower).all() and df['begin_lng'].lt(lng_upper).all(),
            "begin_lng values are not within the defined event_lng_range."
        )
        self.assertTrue(
            df['begin_lat'].gt(lat_lower).all() and df['begin_lat'].lt(lat_upper).all(),
            "begin_lat values are not within the defined event_lat_range."
        )
        self.assertTrue(
            (df['avg_speed'] < self.vars.event_param.event_avg_speed_max).all(),
            "avg_speed values exceed the defined maximum."
        )

    def test_mmsi_column_no_nan(self):
        """Step 3: Check that the mmsi column contains no NaN values."""
        df = self.wrangled_df
        self.assertFalse(
            df['mmsi'].isnull().any(),
            "mmsi column contains NaN values."
        )

    def test_median_precision(self):
        """Step 5: Verify that median longitude and latitude are not within [-1, 1]."""
        df = self.wrangled_df
        median_lng = df['begin_lng'].median()
        median_lat = df['begin_lat'].median()
        self.assertFalse(
            -1 <= median_lng <= 1,
            f"Median of begin_lng is within [-1, 1] (median_lng = {median_lng})."
        )
        self.assertFalse(
            -1 <= median_lat <= 1,
            f"Median of begin_lat is within [-1, 1] (median_lat = {median_lat})."
        )

    def test_unique_event_id(self):
        """Step 6: Ensure event_id values are unique (no duplicates)."""
        df = self.wrangled_df
        duplicates = df['event_id'].duplicated().sum()
        self.assertEqual(
            duplicates, 0,
            f"Found {duplicates} duplicate event_id(s) in the wrangled data."
        )

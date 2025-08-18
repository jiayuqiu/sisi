# -*- encoding: utf-8 -*-
"""
@File    :   events.py
@Time    :   2025/03/02 14:34:39
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   upload events proecessor
"""

import pandas as pd
from pandas.core.frame import DataFrame as PandasDF

from core.infrastructure.data.base import BaseDataProcessor
from core.infrastructure.definition.mapping import EVENT_FIELDS_MAPPING
from core.infrastructure.definition.parameters import (
    ColumnNames as Cn,
    CoordinatePrecision as Cp,
    EventFilterParameters as Efp
)
from core.utils.setup_logger import set_logger

_logger = set_logger(__name__)


class EventsDataProcessor(BaseDataProcessor):
    def __init__(self, csv_file):
        super().__init__(csv_file)
    
    def clean_up(self, data: PandasDF) -> PandasDF:
        """clean up the events data, inspect the precision of longitude and latitude

        steps:
        1. Longitude must be in [-180, 180] and latitude in [-90, 90]. If not, raise an error.
            - check if the coordinate precision is too small.
                - median longitude should not be in [-1, 1]
                - median latitude should not be in [-1, 1]
        2. Filter events based on EventFilterParameters:
            - event_lng_range for beginning longitude
            - event_lat_range for beginning latitude
            - event_avg_speed_max for average speed

        Returns:
            PandasDF: _description_
        """
        # Step 1: Check if the coordinate precision is valid.
        if not data[Cn.lng].between(-180, 180).all():
            raise ValueError("Invalid precision detected: Some longitude values are not in [-180, 180].")
        if not data[Cn.lat].between(-90, 90).all():
            raise ValueError("Invalid precision detected: Some latitude values are not in [-90, 90].")

        # Check if the median values indicate too low precision.
        median_lng = data[Cn.lng].median()
        median_lat = data[Cn.lat].median()
        if -1 <= median_lng <= 1:
            raise ValueError(f"Insufficient precision detected: median longitude is in [-1, 1]. median_lng = {median_lng}")
        if -1 <= median_lat <= 1:
            raise ValueError(f"Insufficient precision detected: median latitude is in [-1, 1]. median_lat = {median_lat}")

        # Step 2: Filter data based on defined parameter ranges.
        filtered_df = data.loc[
            (data[Cn.lng] > Efp.event_lng_range[0]) &
            (data[Cn.lng] < Efp.event_lng_range[1]) &
            (data[Cn.lat] > Efp.event_lat_range[0]) &
            (data[Cn.lat] < Efp.event_lat_range[1]) &
            (data[Cn.sog] < Efp.event_avg_speed_max)
        ]
        return filtered_df
    
    def preprocess(self, data: PandasDF) -> PandasDF:
        """Do pre-precessing for the events data

        steps:
        1. add year, month, day
        2. return the preprocessed data

        Args:
            data (PandasDF): _description_

        Returns:
            PandasDF: _description_
        """
        # Convert the begin_time column and add date parts.
        if Cn.events_data_timestamp in data.columns:
            event_dt = pd.to_datetime(data[Cn.events_data_timestamp], errors='coerce', unit='s')
            if event_dt.isnull().all():
                _logger.warning("All timestamp values could not be converted to datetime.")
            data[Cn.year] = event_dt.dt.year
            data[Cn.month] = event_dt.dt.month
            data[Cn.day] = event_dt.dt.day
        else:
            _logger.warning("Timestamp column not found. Year, month, day columns were not added.")
        
        # if longitude and latitude are not in metrics, convert them to metrics
        precision = 10 ** Cp.precision
        median_lng = data[Cn.lng].median()
        median_lat = data[Cn.lat].median()
        if precision < abs(median_lng):
            data[Cn.lng] = data[Cn.lng].divide(precision)
        if precision < abs(median_lat):
            data[Cn.lat] = data[Cn.lat].divide(precision)
        
        if abs(median_lng) < 1:
            data[Cn.lng] = data[Cn.lng].multiply(precision)
        if abs(median_lat) < 1:
            data[Cn.lat] = data[Cn.lat].multiply(precision)

        return data
    
    def wrangle(self, year: int, month: int) -> PandasDF:
        """TODO: Documentation"""
        for k, _ in EVENT_FIELDS_MAPPING.items():
            if k not in self.df.columns:
                _logger.error(f"Column {k} not found in the data.")

        self.df = self.df.loc[:, list(EVENT_FIELDS_MAPPING.keys())]
        self.df.rename(columns=EVENT_FIELDS_MAPPING, inplace=True)
        formatted_df = self.preprocess(data=self.df)
        formatted_df = formatted_df.loc[
            (formatted_df[Cn.year] == year) & (formatted_df[Cn.month] == month)
        ]

        # # -. add date_id column
        # msg_ts = pd.to_datetime(formatted_df[Cn.evnets_data_timestamp], unit='s')
        # formatted_df[Cn.date_id] = msg_ts.dt.strftime('%Y%m%d').astype(int)

        # -. aggregate by mmsi and date_id
        wrangled_df = self.clean_up(data=formatted_df)

        # -. drop dupilcates on event_id
        wrangled_df.drop_duplicates(subset='event_id', keep='first', inplace=True)
        return wrangled_df

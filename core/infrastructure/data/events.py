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
from core.infrastructure.definition.parameters import ColumnNames as Cn
from core.utils.setup_logger import set_logger

_logger = set_logger(__name__)


class EventsDataProcessor(BaseDataProcessor):
    def __init__(self, csv_file):
        super().__init__(csv_file)
    
    def clean_up(self) -> PandasDF:
        """clean up the events data, inspect the precision of longitude and latitude

        steps:
        1. longitude should be in [-180, 180], most of the longitude should be in [90, 140]
        2. latitude should be in [-90, 90], most of the latitude should be in [10, 60]
        3. if the precision is invalid, e.g. longitude is 120000000, latitude is 30000000, we should raise an error
        4. return the cleaned data

        Returns:
            PandasDF: _description_
        """
    
    def preprocess(self, data: PandasDF) -> PandasDF:
        """Do pre-precessing for the events data

        steps:
        1. rename columns by mapping
        2. add year, month, day
        3. return the preprocessed data

        Args:
            data (PandasDF): _description_

        Returns:
            PandasDF: _description_
        """
        data = data.rename(columns=EVENT_FIELDS_MAPPING)

        # Convert the begin_time column and add date parts.
        if Cn.timestamp in data.columns:
            data[Cn.timestamp] = pd.to_datetime(data['timestamp'], errors='coerce')
            if data[Cn.timestamp].isnull().all():
                _logger.warning("All timestamp values could not be converted to datetime.")
            data[Cn.year] = data[Cn.timestamp].dt.year
            data[Cn.month] = data[Cn.timestamp].dt.month
            data[Cn.day] = data[Cn.timestamp].dt.day
        else:
            _logger.warning("Timestamp column not found. Year, month, day columns were not added.")

        return data
    
    def wrangle(self) -> PandasDF:
        """TODO: Documentation"""


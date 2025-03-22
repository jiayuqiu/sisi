"""
@File    :   statics.py
@Time    :   2025/02/23 12:28:49
@Author  :   jerry
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :

This script is used for statics data processing.

Due to the speciality of statics data, `mmsi` is just the ID of AIS device. It is not able to relate to a specific ship,
because when the owner sails the ship to another, the new owner might replace the old AIS device with a new one. And
then the mmsi number will be changed.

This module is aim to process statics data to merge statics data of each `mmsi` into one record per day.

steps:
1. load data from csv files
2. group by mmsi
3. clean up
4. store
"""

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame as PandasDF
from tqdm import tqdm

from core.infrastructure.definition.mapping import STATICS_COLUMNS_MAPPING
from core.infrastructure.definition.parameters import StaticsCleanThreshold as SCt
from core.infrastructure.data.base import BaseDataProcessor
from core.infrastructure.definition.parameters import (
    ColumnNames as Cn,
    Suffix as Sf
)


class StaticsDataProcessor(BaseDataProcessor):
    def __init__(self, csv_file: str):
        super().__init__(csv_file)

    def clean_up(self, data: PandasDF) -> PandasDF:
        """
        Clean the statics data

        Key Steps:
        1. If filter_null is True, filter out null or erroneous rows.
        2. Calculate the length_width_ratio as length divided by width.
        3. Filter rows based on the minimum and maximum ratio thresholds.
        4. Group data by mmsi and date_id, then compute variance for ship length and width.
        5. Filter valid mmsi records and remove duplicate entries.

        :param data: A pandas DataFrame containing the statics data.
        :return: A cleaned pandas DataFrame with valid statics data.
        """
        # *. calculate length_width_ratio which indicates the shape of the ship.
        data.loc[:, Cn.length_width_ratio] = data.apply(lambda row: row[Cn.statics_data_length] / row[Cn.statics_data_width], axis=1)
        data = data.loc[(data[Cn.length_width_ratio] >= SCt.min_ratio_threshold) &
                        (data[Cn.length_width_ratio] <= SCt.max_ratio_threshold)]

        # *. some ships have multiple pairs of length and width, we need to filter them.
        static_info_list = []
        group_cols = [Cn.mmsi, Cn.date_id]
        group_df = data.groupby(group_cols)
        total_unique = group_df.ngroups
        mmsi_group_bar = tqdm(group_df, total=total_unique, desc="Doing cleaning statics")
        for (mmsi, date_id), group in mmsi_group_bar:
            mmsi_info = {Cn.mmsi: mmsi}
            length_var = np.var(group[Cn.statics_data_length])
            width_var = np.var(group[Cn.statics_data_width])
            mmsi_info[f"{Cn.statics_data_length}{Sf.var}"] = length_var
            mmsi_info[f"{Cn.statics_data_width}{Sf.var}"] = width_var
            mmsi_info[Cn.date_id] = date_id
            static_info_list.append(mmsi_info)

        static_statistical_df = pd.DataFrame(static_info_list)

        # select valid mmsi data
        data = data.loc[data[Cn.mmsi].isin(static_statistical_df[Cn.mmsi])]

        # select latest statics data
        data.drop_duplicates(subset=[Cn.mmsi, Cn.date_id], keep='last', inplace=True)
        return data

    def preprocess(self, data: PandasDF) -> PandasDF:
        """
        Filter null and error data from statics data.

        :param data: statics data
        :return: filtered statics data
        """
        # -. filter receivetime
        data[Cn.statics_data_timestamp] = pd.to_numeric(data[Cn.statics_data_timestamp], errors='coerce')
        data.dropna(subset=[Cn.statics_data_timestamp], inplace=True)
        data[Cn.statics_data_timestamp] = data[Cn.statics_data_timestamp].astype(int)
        data = data.loc[data[Cn.statics_data_timestamp].notnull()]

        # -. filter mmsi
        data[Cn.mmsi] = pd.to_numeric(data[Cn.mmsi], errors='coerce')
        data.dropna(subset=[Cn.mmsi], inplace=True)
        data[Cn.mmsi] = data[Cn.mmsi].astype(int)
        data = data.loc[data[Cn.mmsi].notnull()]

        # -. filter ship_name
        data = data.loc[data[Cn.ship_name] != '""']
        data = data.loc[data[Cn.ship_name].notnull()]

        data = data.loc[data[Cn.statics_data_length] != '""']
        data = data.loc[data[Cn.statics_data_width] != '""']
        data = data.loc[data[Cn.ship_type] != '""']
        
        data[Cn.statics_data_length] = data[Cn.statics_data_length].astype(float)
        data[Cn.statics_data_width] = data[Cn.statics_data_width].astype(float)
        data[Cn.ship_type] = data[Cn.ship_type].astype(float)

        data = data.loc[data[Cn.statics_data_length] > 0]
        data = data.loc[data[Cn.statics_data_width] > 0]
        return data

    def wrangle(self, year: int, month: int) -> PandasDF:
        # -. rename columns by mapping
        self.df = self.df.loc[:, list(STATICS_COLUMNS_MAPPING.keys())]
        self.df.rename(columns=STATICS_COLUMNS_MAPPING, inplace=True)
        formatted_df = self.preprocess(data=self.df)

        # -. add date_id column
        msg_ts = pd.to_datetime(formatted_df[Cn.statics_data_timestamp], unit='s')
        formatted_df[Cn.date_id] = msg_ts.dt.strftime('%Y%m%d').astype(int)
        formatted_df[Cn.month] = msg_ts.dt.strftime('%m').astype(int)
        formatted_df[Cn.year] = msg_ts.dt.strftime('%Y').astype(int)
        formatted_df = formatted_df.loc[
            (formatted_df[Cn.year] == year) & (formatted_df[Cn.month] == month)
        ]

        # -. aggregate by mmsi and date_id
        wrangled_df = self.clean_up(data=formatted_df)
        return wrangled_df

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


class StaticsDataProcessor:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        # -. load as pandas dataframe
        self.df = pd.read_csv(self.csv_path)

    @staticmethod
    def clean_up(data: PandasDF):
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
        data.loc[:, 'length_width_ratio'] = data.apply(lambda row: row['length'] / row['width'], axis=1)
        data = data.loc[(data['length_width_ratio'] >= SCt.min_ratio_threshold) &
                        (data['length_width_ratio'] <= SCt.max_ratio_threshold)]

        # *. some ships have multiple pairs of length and width, we need to filter them.
        static_info_list = []
        group_cols = ['mmsi', 'date_id']
        group_df = data.groupby(group_cols)
        total_unique = group_df.ngroups
        mmsi_group_bar = tqdm(group_df, total=total_unique, desc="Doing aggregate statics")
        for (mmsi, date_id), group in mmsi_group_bar:
            mmsi_info = {'mmsi': mmsi}
            length_var = np.var(group['length'])
            width_var = np.var(group['width'])
            mmsi_info['length_var'] = length_var
            mmsi_info['width_var'] = width_var
            mmsi_info['date_id'] = date_id
            static_info_list.append(mmsi_info)

        static_statistical_df = pd.DataFrame(static_info_list)

        # select valid mmsi data
        data = data.loc[data['mmsi'].isin(static_statistical_df['mmsi'])]

        # select latest statics data
        data.drop_duplicates(subset=['mmsi', 'date_id'], keep='last', inplace=True)
        return data

    @staticmethod
    def preprocess(data: pd.DataFrame) -> pd.DataFrame:
        """
        Filter null and error data from statics data.

        :param data: statics data
        :return: filtered statics data
        """
        # -. filter receivetime
        data['receivetime'] = pd.to_numeric(data['receivetime'], errors='coerce')
        data.dropna(subset=['receivetime'], inplace=True)
        data['receivetime'] = data['receivetime'].astype(int)
        data = data.loc[data['receivetime'].notnull()]

        # -. filter mmsi
        data['mmsi'] = pd.to_numeric(data['mmsi'], errors='coerce')
        data.dropna(subset=['mmsi'], inplace=True)
        data['mmsi'] = data['mmsi'].astype(int)
        data = data.loc[data['mmsi'].notnull()]

        # -. filter ship_name
        data = data.loc[data['ship_name'] != '""']
        data = data.loc[data['ship_name'].notnull()]

        data = data.loc[data['length'] != '""']
        data = data.loc[data['width'] != '""']
        data = data.loc[data['ship_type'] != '""']
        
        data['length'] = data['length'].astype(float)
        data['width'] = data['width'].astype(float)
        data['ship_type'] = data['ship_type'].astype(float)

        data = data.loc[data['length'] > 0]
        data = data.loc[data['width'] > 0]
        return data

    def wrangle(self,) -> PandasDF:
        # -. rename columns by mapping
        self.df = self.df.loc[:, list(STATICS_COLUMNS_MAPPING.keys())]
        self.df.rename(columns=STATICS_COLUMNS_MAPPING, inplace=True)
        formatted_df = self.preprocess(data=self.df)

        # -. add date_id column
        msg_ts = pd.to_datetime(formatted_df["receivetime"], unit='s')
        formatted_df['date_id'] = msg_ts.dt.strftime('%Y%m%d').astype(int)

        # -. aggregate by mmsi and date_id
        wrangled_df = self.clean_up(data=formatted_df)
        return wrangled_df

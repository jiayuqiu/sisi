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
from tqdm import tqdm

from core.infrastructure.definition.mapping import STATICS_COLUMNS_MAPPING
from core.infrastructure.definition.parameters import StaticsCleanThreshold as SCt


class StaticsDataProcessor:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def clean_up(cls, df, filter_null: bool = False):
        """
        There are error data in static_data.
        This function is aim to filter error data.

        Valid Data Request:
        0. length and width are numerical.
        1. contains length and width
        2. length ∈ (0, +∞)
        3. width ∈ (0, +∞)

        :param df: original statics data
        :param clean: whether to clean data in manual
        :return: cleaned statics data
        """
        # *. convert length, width, shiptype to float
        if filter_null:
            df = cls.filter_null_error(df)

        # *. calculate length_width_ratio which indicates the shape of the ship.
        df.loc[:, 'length_width_ratio'] = df.apply(lambda row: row['length'] / row['width'], axis=1)
        df = df.loc[(df['length_width_ratio'] >= SCt.min_ratio_threshold) &
                    (df['length_width_ratio'] <= SCt.max_ratio_threshold)]

        # *. some ships have multiple pairs of length and width, we need to filter them.
        static_info_list = []
        group_cols = ['mmsi', 'date_id']
        group_df = df.groupby(group_cols)
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
        df = df.loc[df['mmsi'].isin(static_statistical_df['mmsi'])]

        # select latest statics data
        df.drop_duplicates(subset=['mmsi', 'date_id'], keep='last', inplace=True)
        return df


    def filter_null_error(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter null and error data from statics data.

        :param df: statics data
        :return: filtered statics data
        """
        # -. filter receivetime
        df['receivetime'] = pd.to_numeric(df['receivetime'], errors='coerce')
        df.dropna(subset=['receivetime'], inplace=True)
        df['receivetime'] = df['receivetime'].astype(int)
        df = df.loc[df['receivetime'].notnull()]

        # -. filter mmsi
        df['mmsi'] = pd.to_numeric(df['mmsi'], errors='coerce')
        df.dropna(subset=['mmsi'], inplace=True)
        df['mmsi'] = df['mmsi'].astype(int)
        df = df.loc[df['mmsi'].notnull()]

        # -. filter ship_name
        df = df.loc[df['ship_name'] != '""']
        df = df.loc[df['ship_name'].notnull()]

        df = df.loc[df['length'] != '""']
        df = df.loc[df['width'] != '""']
        df = df.loc[df['ship_type'] != '""']
        
        df['length'] = df['length'].astype(float)
        df['width'] = df['width'].astype(float)
        df['ship_type'] = df['ship_type'].astype(float)

        df = df.loc[df['length'] > 0]
        df = df.loc[df['width'] > 0]
        return df

    def load_data(self) -> pd.DataFrame:
        """
        Load statics data from csv file.

        :return: statics data
        """
        # -. load as pandas dataframe
        df = pd.read_csv(self.csv_path)

        # -. rename columns by mapping
        df = df.loc[:, list(STATICS_COLUMNS_MAPPING.keys())]
        df.rename(columns=STATICS_COLUMNS_MAPPING, inplace=True)

        # -. filter null data
        df = self.filter_null_error(df)

        # -. add date_id column
        msg_ts = pd.to_datetime(df['receivetime'], unit='s')
        df['date_id'] = msg_ts.dt.strftime('%Y%m%d').astype(int)

        # -. aggregate by mmsi and date_id
        df = self.clean_up(df)
        return df

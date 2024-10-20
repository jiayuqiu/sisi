"""
@Author  ：  Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  filter.py
@DateTime:  20/10/2024 4:32 pm
@DESC    :  filter error statics data
"""

import pandas as pd
from pandas.core.frame import DataFrame

import numpy as np


def clean_up(df: DataFrame) -> DataFrame:
    """
    There are error data in static_data.
    This function is aim to filter error data.

    Valid Data Request:
    0. length and breadth are numerical.
    1. contains length and breadth
    2. length ∈ (0, +∞)
    3. breadth ∈ (0, +∞)

    :param df: original statics data
    :return: cleaned statics data
    """
    # *. convert length, breadth, shiptype to float
    static_df = df.loc[df['length']!='""']
    static_df = static_df.loc[static_df['breadth']!='""']
    static_df = static_df.loc[static_df['shiptype']!='""']

    static_df.loc[:, 'length'] = static_df['length'].astype(float)
    static_df.loc[:, 'breadth'] = static_df['breadth'].astype(float)
    static_df.loc[:, 'shiptype'] = static_df['shiptype'].astype(float)

    static_df = static_df.loc[static_df['length'] > 0]
    static_df = static_df.loc[static_df['breadth'] > 0]

    # *. calculate length_breadth_ratio which indicates the shape of the ship.
    # if length_breadth_ratio is too sharp, it may be an error data.
    # set ±5% as confidence interval to filter error data.
    static_df.loc[:, 'length_breadth_ratio'] = static_df.apply(lambda row: row['length'] / row['breadth'], axis=1)
    min_ratio_threshold = np.percentile(static_df['length_breadth_ratio'], 5)  # should be equal to 3
    max_ratio_threshold = np.percentile(static_df['length_breadth_ratio'], 95)  # should be equal to 7.1

    static_df = static_df.loc[(static_df['length_breadth_ratio'] > min_ratio_threshold) &
                              (static_df['length_breadth_ratio'] < max_ratio_threshold)]

    # *. some ships have multiple pairs of length and breadth, we need to filter them.
    static_info_list = []
    for mmsi, group in static_df.groupby('mmsi'):
        mmsi_info = {'mmsi': mmsi}
        length_var = np.var(group['length'])
        breadth_var = np.var(group['breadth'])
        mmsi_info['length_var'] = length_var
        mmsi_info['breadth_var'] = breadth_var
        static_info_list.append(mmsi_info)

    static_statistical_df = pd.DataFrame(static_info_list)
    static_statistical_df = static_statistical_df.loc[(static_statistical_df['length_var'] == 0) &
                                                      (static_statistical_df['breadth_var'] == 0)]

    # select valid mmsi data
    static_df = static_df.loc[static_df['mmsi'].isin(static_statistical_df['mmsi'])]

    # select latest statics data
    static_df.drop_duplicates(subset=['mmsi'], keep='last', inplace=True)
    return static_df
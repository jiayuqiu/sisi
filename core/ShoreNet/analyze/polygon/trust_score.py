"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  trust_score.py
@DateTime:  16/11/2024 8:52 pm
@DESC    :  trust score calculation
            1. density = number of events / mmsi nunique
"""

import numpy as np


def trust_score(df):
    """
    trust score calculation
    :param df: event dataframe
    :return: trust score dataframe
    """
    cluster_gdf = df.groupby('cluster_id')

    density_list = []
    for cluster_id, group in cluster_gdf:
        density_list.append(group.shape[0] / group['mmsi'].nunique())

    return np.mean(density_list)


def effectual_event_percentage(normalized_distribute):
    """
    calculate effectual event percentage

    :param normalized_distribute: event dataframe
    :return: effectual event percentage
    """
    _per = 0
    if 'record_poly' in normalized_distribute:
        _per += normalized_distribute['record_poly']

    if 'stop_event_poly' in normalized_distribute:
        _per += normalized_distribute['stop_event_poly']

    return _per * 100

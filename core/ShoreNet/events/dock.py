"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  dock.py
@DateTime:  19/10/2024 12:52 am
"""

from typing import Union

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from sklearn.cluster import DBSCAN

from core.ShoreNet.definitions.variables import VariablesManager


def cluster_dock_polygon_dbscan(
        events_df: DataFrame,
        var: VariablesManager
) -> DataFrame:
    # dbscan all_coal_df
    coords = events_df[['lng', 'lat']].values

    # DBSCAN clustering
    db = DBSCAN(
        eps=var.dbscan_params["eps"],
        min_samples=var.dbscan_params["min_samples"],
        algorithm='ball_tree',
        metric='haversine'
    ).fit(np.radians(coords))

    # Add cluster labels to the DataFrame
    events_df['cluster'] = db.labels_

    # filter noise cluster
    events_df = events_df[events_df['cluster'] != -1]

    # filter by ship count and event count
    cleaned_cluster_id_list = []
    for cluster_id, group in events_df.groupby("cluster"):
        if ((group['mmsi'].nunique() > var.event_param.cluster_unique_mmsi_min_count) &
                (group.shape[0] > var.event_param.cluster_events_min_count)):
            cleaned_cluster_id_list.append(cluster_id)
    cleaned_event_df = events_df.loc[events_df['cluster'].isin(cleaned_cluster_id_list)]
    return cleaned_event_df


def pair_event_polygon(event_row: pd.Series, dock_list: list) -> Union[int, None]:
    from core.ShoreNet.utils.geo import point_poly, get_geodist
    for polygon in dock_list:
        dst_list = []
        for d_lat, d_lng in polygon['polygon']:
            geodist = get_geodist(
                lon1=event_row['begin_lng'],
                lat1=event_row['begin_lat'],
                lon2=d_lng,
                lat2=d_lat
            )
            dst_list.append(geodist)

        if min(dst_list) < 20:
            if point_poly(lng=event_row['begin_lng'], lat=event_row['begin_lat'], polygon_points=polygon['polygon']):
                return polygon['dock_id']

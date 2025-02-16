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

from core.ShoreNet.definitions.variables import VariablesManager, EventFilterParameters
from core.ShoreNet.definitions.parameters import ColumnNames


def cluster_dock_polygon_dbscan(
        events_df: DataFrame,
        vars: VariablesManager
) -> DataFrame:
    """
    this function is used to cluster dock polygon by dbscan

    :param events_df:
    :param var:
    :return:
    """
    # dbscan all_coal_df
    coords = events_df[[ColumnNames.lng_column_name, ColumnNames.lat_column_name]].values

    # DBSCAN clustering
    db = DBSCAN(
        eps=vars.dbscan_params["eps"],
        min_samples=vars.dbscan_params["min_samples"],
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
        if ((group['mmsi'].nunique() > vars.event_param.cluster_unique_mmsi_min_count) &
                (group.shape[0] > vars.event_param.cluster_events_min_count)):
            cleaned_cluster_id_list.append(cluster_id)
    cleaned_event_df = events_df.loc[events_df['cluster'].isin(cleaned_cluster_id_list)]
    return cleaned_event_df


def map_event_polygon(event_row: pd.Series, dock_list: list) -> Union[int, None]:
    from core.ShoreNet.utils.geo import point_poly, get_geodist
    if event_row["event_id"] == "20230101000030201202010":
        print(1)
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

        if min(dst_list) < EventFilterParameters.polygon_event_max_distance:
            if point_poly(
                lng=event_row[ColumnNames.lng_column_name],
                lat=event_row[ColumnNames.lat_column_name],
                polygon_points=polygon['polygon']
            ):
                return polygon['dock_id']

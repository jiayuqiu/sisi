"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  dock.py
@DateTime:  19/10/2024 12:52 am
"""

from typing import Union

import numpy as np
from numba import njit, prange
import pandas as pd
from pandas.core.frame import DataFrame
from sklearn.cluster import DBSCAN

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager, EventFilterParameters
from core.infrastructure.definition.parameters import ColumnNames
from core.ShoreNet.utils.geo import haversine, point_in_poly


def cluster_dock_polygon_dbscan(
        events_df: DataFrame,
        vars: ShoreNetVariablesManager
) -> DataFrame:
    """
    this function is used to cluster dock polygon by dbscan

    :param events_df:
    :param var:
    :return:
    """
    # dbscan all_coal_df
    coords = events_df[[ColumnNames.lng, ColumnNames.lat]].values

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


@njit(parallel=True)
def match_polygons(points, polygons):
    n_points = points.shape[0]
    result = -np.ones(n_points, dtype=np.int64)
    for i in prange(n_points):
        lng = points[i, 0]
        lat = points[i, 1]
        for j in range(len(polygons)):
            poly = polygons[j]
            if point_in_poly(lat, lng, poly):
                result[i] = j  # here polygon index is used as the dock tag
                break
    return result


@njit(parallel=True)
def map_event_polygon_numba(event_begin_lng, event_begin_lat, event_lng, event_lat,
                            polygons, dock_ids, polygon_event_max_distance):
    # n_polys = len(polygons)
    # for i in prange(n_polys):
    #     poly = polygons[i]
    #     n_points = poly.shape[0]
    #     min_dst = 1e12
    #     for j in range(n_points):
    #         d_lat = poly[j, 0]
    #         d_lng = poly[j, 1]
    #         geodist = haversine(event_begin_lng, event_begin_lat, d_lng, d_lat)
    #         if geodist < min_dst:
    #             min_dst = geodist
    #     if min_dst < polygon_event_max_distance:
    #         if point_in_poly(event_lng, event_lat, poly):
    #             return dock_ids[i]
    # return -1  # no matching polygon found
    raise NotImplementedError(
        "map_event_polygon_numba is not implemented yet. Please use match_polygons instead."
        "Keep this function for future reference."
    )

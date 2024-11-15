"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  departure_destination_docks.py
@DateTime:  9/11/2024 11:51 pm
@DESC    :  map departure and destination docks for events
"""
import numpy as np
import pandas as pd
from typing import Any

from core.ShoreNet.definitions.parameters import ColumnNames as Cn
from core.ShoreNet.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def map_dock_pairs(df, ) -> pd.DataFrame:
    """
    TODO: map departure and destination docks for events

    :param df: events with polygons dataframe.
    :return:
    """
    # -. check if exists data without dock_polygon_id.
    if df['coal_dock_id'].isnull().sum() > 0:
        raise ValueError("There are still events without dock_polygon_id.")

    # -. get dock pairs
    records_list: list = []
    gdf = df.groupby('mmsi')
    g_n = len(gdf)
    n = 1
    for mmsi, group in gdf:
        # _logger.info(f"processing {mmsi}, {g_n - n} left.")
        group.sort_values('begin_time', inplace=True)
        group.loc[:, 'next_polygon_id'] = group['coal_dock_id'].shift(-1)
        group.loc[:, 'next_polygon_arrive_time'] = group['begin_time'].shift(-1)
        group.loc[:, 'next_polygon_lng'] = group[Cn.lng_column_name].shift(-1)
        group.loc[:, 'next_polygon_lat'] = group[Cn.lat_column_name].shift(-1)
        group.loc[:, 'next_polygon_arrive_time'] = group['begin_time'].shift(-1)

        for _g_row_id, _row in group.iterrows():
            if _row['coal_dock_id'] != _row['next_polygon_id']:
                # stop_duration = (_row['end_time'] - _row['begin_time']) / 86400
                sail_duration = _row['next_polygon_arrive_time'] - _row['end_time']
                if (np.isnan(_row['coal_dock_id'])) or (np.isnan(_row['next_polygon_id'])):
                    continue

                records_list.append(
                    {
                        'mmsi': mmsi,
                        'departure_dock_id': int(_row['coal_dock_id']),
                        'departure_lng': _row[Cn.lng_column_name],
                        'departure_lat': _row[Cn.lat_column_name],
                        'destination_dock_id': int(_row['next_polygon_id']),
                        'destination_lng': _row['next_polygon_lng'],
                        'destination_lat': _row['next_polygon_lat'],
                        'sail_duration': int(sail_duration)
                    }
                )

        n += 1
    return pd.DataFrame(records_list)
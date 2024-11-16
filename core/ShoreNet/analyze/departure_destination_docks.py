"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  departure_destination_docks.py
@DateTime:  9/11/2024 11:51 pm
@DESC    :  map departure and destination docks for events
"""
import numpy as np
import pandas as pd
from multiprocessing import Pool

from core.ShoreNet.definitions.parameters import ColumnNames as Cn
from core.ShoreNet.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def calculate_od_paris_mmsi(df: pd.DataFrame) -> list[dict]:
    """
    calculate od paris for one mmsi
    :param df: one mmsi events dataframe
    :return: records list
    """
    _logger.info(f"mmsi -> {df['mmsi'].iloc[0]} is mapping departure and destination docks...")
    records_list: list = []
    df.sort_values('begin_time', inplace=True)
    df.loc[:, 'next_polygon_id'] = df['coal_dock_id'].shift(-1)
    df.loc[:, 'next_polygon_arrive_time'] = df['begin_time'].shift(-1)
    df.loc[:, 'next_polygon_lng'] = df[Cn.lng_column_name].shift(-1)
    df.loc[:, 'next_polygon_lat'] = df[Cn.lat_column_name].shift(-1)
    df.loc[:, 'next_polygon_arrive_time'] = df['begin_time'].shift(-1)

    for _row_id, _row in df.iterrows():
        if _row['coal_dock_id'] != _row['next_polygon_id']:
            # stop_duration = (_row['end_time'] - _row['begin_time']) / 86400
            sail_duration = _row['next_polygon_arrive_time'] - _row['end_time']
            if (np.isnan(_row['coal_dock_id'])) or (np.isnan(_row['next_polygon_id'])):
                continue

            records_list.append(
                {
                    'mmsi': int(_row["mmsi"]),
                    'departure_dock_id': int(_row['coal_dock_id']),
                    'departure_lng': float(_row[Cn.lng_column_name]),
                    'departure_lat': float(_row[Cn.lat_column_name]),
                    'destination_dock_id': int(_row['next_polygon_id']),
                    'destination_lng': float(_row['next_polygon_lng']),
                    'destination_lat': float(_row['next_polygon_lat']),
                    'sail_duration': int(sail_duration)
                }
            )
    return records_list

def map_dock_pairs(df, process_workders=8) -> pd.DataFrame:
    """
    map departure and destination docks for events

    :param df: events with polygons dataframe.
    :return:
    """
    # -. check if exists data without dock_polygon_id.
    if df['coal_dock_id'].isnull().sum() > 0:
        raise ValueError("There are still events without dock_polygon_id.")

    # -. get dock pairs
    records_list: list = []
    gdf = df.groupby('mmsi')
    _logger.info("Start pairing departure and destination docks...")
    with Pool(process_workders) as pool:
        results = pool.map(
            calculate_od_paris_mmsi,
            [group for _, group in gdf]
        )

    records_list = [item for sublist in results for item in sublist]
    return pd.DataFrame(records_list)

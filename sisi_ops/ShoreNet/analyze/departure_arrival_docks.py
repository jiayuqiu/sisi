"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  departure_arrival_docks.py
@DateTime:  9/11/2024 11:51 pm
@DESC    :  map departure and arrival docks for events
"""
import datetime
import numpy as np
import pandas as pd
from multiprocessing import Pool

from sisi_ops.infrastructure.definition.parameters import (
    ColumnNames as Cn
)
from sisi_ops.python.utils.dt import get_quarter
from sisi_ops.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def map_od_paris_mmsi(df: pd.DataFrame) -> list[dict]:
    """Calculate OD pairs for a single MMSI.

    Args:
        df (pd.DataFrame): DataFrame containing events for one MMSI. Required columns:
            'mmsi', 'begin_time', 'end_time', 'coal_dock_id', and coordinate
            columns referenced by Cn.lng and Cn.lat.

    Returns:
        list[dict]: List of dictionaries, each representing an OD record with keys:
            'mmsi', 'departure_dock_id', 'arrival_dock_id', 'departure_time',
            'arrival_time', 'departure_year', 'arrival_year', 'departure_month',
            'arrival_month', 'departure_quarter', 'arrival_quarter',
            'departure_lng', 'departure_lat', 'arrival_lng', 'arrival_lat',
            and 'sail_duration'.
    """
    # _logger.info(f"mmsi -> {df['mmsi'].iloc[0]} is mapping departure and arrival docks...")
    records_list: list = []
    df.sort_values('begin_time', inplace=True)
    df.loc[:, 'next_polygon_id'] = df['coal_dock_id'].shift(-1)
    df.loc[:, 'next_polygon_arrive_time'] = df['begin_time'].shift(-1)
    df.loc[:, 'next_polygon_lng'] = df[Cn.lng].shift(-1)
    df.loc[:, 'next_polygon_lat'] = df[Cn.lat].shift(-1)
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
                    'departure_time': _row['begin_time'],
                    'departure_year': datetime.datetime.fromtimestamp(_row['begin_time']).year,
                    'departure_month': datetime.datetime.fromtimestamp(_row['begin_time']).month,
                    'departure_quarter': get_quarter(_row['begin_time']),
                    'departure_lng': float(_row[Cn.lng]),
                    'departure_lat': float(_row[Cn.lat]),
                    'arrival_dock_id': int(_row['next_polygon_id']),
                    'arrival_time': _row['next_polygon_arrive_time'],
                    'arrival_year': datetime.datetime.fromtimestamp(_row['next_polygon_arrive_time']).year,
                    'arrival_month': datetime.datetime.fromtimestamp(_row['next_polygon_arrive_time']).month,
                    'arrival_quarter': get_quarter(_row['next_polygon_arrive_time']),
                    'arrival_lng': float(_row['next_polygon_lng']),
                    'arrival_lat': float(_row['next_polygon_lat']),
                    'sail_duration': int(sail_duration)
                }
            )
    return records_list

def map_dock_pairs(df, process_workders=8) -> pd.DataFrame:
    """Map departure and arrival docks for events.

    Args:
        df (pd.DataFrame): Events dataframe with polygon information (must contain
            'coal_dock_id' and 'mmsi').
        process_workders (int): Number of worker processes to use for parallel processing.

    Returns:
        pd.DataFrame: DataFrame of mapped OD records. Each record contains fields
            such as 'mmsi', 'departure_dock_id', 'arrival_dock_id', 'departure_time',
            'arrival_time', 'departure_year', 'arrival_year', etc.

    Raises:
        ValueError: If there are events without a dock polygon id in 'coal_dock_id'.
    """
    # -. check if exists data without dock_polygon_id.
    if df['coal_dock_id'].isnull().sum() > 0:
        raise ValueError("There are still events without dock_polygon_id.")

    # -. get dock pairs
    records_list: list = []
    gdf = df.groupby('mmsi')
    _logger.info("Start pairing departure and arrival docks...")
    with Pool(process_workders) as pool:
        results = pool.map(
            map_od_paris_mmsi,
            [group for _, group in gdf]
        )

    records_list = [item for sublist in results for item in sublist]
    return pd.DataFrame(records_list)


def calculate_dd_event_count_month(year: int, month: int, dock_id: int, con) -> (int, int):
    """
    Deprecated: Calculate departure and arrival ship count for a dock for a given month.

    Args:
        year (int): Year of the events.
        month (int): Month of the events.
        dock_id (int): Identifier of the dock.
        con: Database connection object (e.g., SQLAlchemy engine/connection).

    Returns:
        tuple[int, int]: A tuple containing:
            - departure_ship_count (int): Number of departure events for the dock in the given month.
            - arrival_ship_count (int): Number of arrival events for the dock in the given month.
    """
    departure_query = f"""
             SELECT
                 COUNT(1) as event_count
             FROM
                 sisi.data_od_pairs
             WHERE
                 departure_year = {year} AND departure_month = {month} AND departure_dock_id = {dock_id}
             """
    departure_ship_count_df = pd.read_sql(
        sql=departure_query,
        con=con
    )
    departure_ship_count = departure_ship_count_df['event_count'].iloc[0]

    arrival_query = f"""
             SELECT
                 COUNT(1) as event_count
             FROM
                 sisi.data_od_pairs
             WHERE
                 arrival_year = {year} AND arrival_month = {month} AND arrival_dock_id = {dock_id}
             """
    arrival_ship_count_df = pd.read_sql(
        sql=arrival_query,
        con=con
    )
    arrival_ship_count = arrival_ship_count_df['event_count'].iloc[0]
    return departure_ship_count, arrival_ship_count


def calculate_dd_event_count_quarter(year: int, quarter: int, dock_id: int, con) -> (int, int):
    """
    Deprecated: Calculate departure and arrival ship count for a dock for a given quarter.

    Args:
        year (int): Year of the events.
        quarter (int): Quarter number (1-4).
        dock_id (int): Identifier of the dock.
        con: Database connection object (e.g., SQLAlchemy engine/connection).

    Returns:
        tuple[int, int]: A tuple containing:
            - departure_ship_count (int): Number of departure events for the dock in the given quarter.
            - arrival_ship_count (int): Number of arrival events for the dock in the given quarter.
    """
    departure_query = f"""
             SELECT
                 COUNT(1) as event_count
             FROM
                 sisi.data_od_pairs
             WHERE
                 departure_year = {year} AND departure_quarter = {quarter} AND departure_dock_id = {dock_id}
             """
    departure_ship_count_df = pd.read_sql(
        sql=departure_query,
        con=con
    )
    departure_ship_count = departure_ship_count_df['event_count'].iloc[0]

    arrival_query = f"""
             SELECT
                 COUNT(1) as event_count
             FROM
                 sisi.data_od_pairs
             WHERE
                 arrival_year = {year} AND departure_quarter = {quarter} AND arrival_dock_id = {dock_id}
             """
    arrival_ship_count_df = pd.read_sql(
        sql=arrival_query,
        con=con
    )
    arrival_ship_count = arrival_ship_count_df['event_count'].iloc[0]
    return departure_ship_count, arrival_ship_count

"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  main_features_trust_score.py
@DateTime:  23/11/2024 9:48 pm
@DESC    :  calculate middle data for trust score calculation.
            1. in researching stage, get as more as possible features for trust score calculation.
"""


import os
import argparse

from pandas.errors import SettingWithCopyWarning
import warnings


warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.events.generic.tools import (
    load_events_with_dock,
    load_dock_polygon
)
from core.ShoreNet.events.filter import clean_up_events
from core.ShoreNet.analyze.polygon.trust_score import trust_score, effectual_event_percentage
from core.ShoreNet.analyze.departure_arrival_docks import calculate_dd_event_count_month, \
    calculate_dd_event_count_quarter

from core.ShoreNet.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def process_middle_features_month(
    dock_id: int,
    events_df: pd.DataFrame,
    year: int,
    var: VariablesManager,
    skip_event_count: int = 5
):
    """
    calculate middle features for trust score calculation

    :param dock_id: dock id
    :param events_df: events data frame
    :param year: year
    :param var: VariablesManager
    :param skip_event_count: skip event count
    :return:
    """
    features_rows: list = []
    for month in range(1, 12+1):
        _month_dock_events = events_df.loc[events_df['month'] == month]
        _month_dock_events.loc[:, 'stop_duration'] = _month_dock_events.apply(
            lambda _row: _row['end_time'] - _row['begin_time'], axis=1)
        _month_dock_events = _month_dock_events.loc[
            (_month_dock_events['stop_duration'] > 1800) & (_month_dock_events['stop_duration'] < 7 * 24 * 3600)]

        if _month_dock_events.shape[0] < skip_event_count:
            # -. record features
            features_rows.append(
                {
                    'dock_id': dock_id,
                    'month': month,
                    'avg_event_count_per_mmsi': None,
                    'departure_count': None,
                    'arrival_count': None,
                    'low_speed_stop_event_percentage': None
                }
            )
            _logger.info(f"dock_id: {dock_id}, month: {month} event count is less than {skip_event_count}, skip.")
            continue

        # -. dbscan. to find effectual area
        coords = _month_dock_events[[var.column_names.lng_column_name, var.column_names.lat_column_name]].values
        db = DBSCAN(
            eps=0.1 / var.geo_param.kms_per_radian,  # convert 0.1m to radian.
            min_samples=20,
            algorithm='ball_tree',
            metric='haversine'
        ).fit(np.radians(coords))
        _month_dock_events.loc[:, 'cluster_id'] = db.labels_

        # -. calculate trust score (event_count_per_mmsi)
        _month_dock_trust_score = trust_score(_month_dock_events)

        # -. calculate departure&arrival ship count
        departure_event_count, arrival_event_count = calculate_dd_event_count_month(
            year=year,
            month=month,
            dock_id=dock_id,
            con=var.engine
        )

        # -. calculate low_speed / stop_event
        event_category_distribute_percentage = _month_dock_events['event_categories'].value_counts(normalize=True)
        percentage_val = effectual_event_percentage(event_category_distribute_percentage)

        # -. calculate low_speed / stop_event in dbscan cluster
        for cluster_id, group in _month_dock_events.groupby('cluster_id'):
            event_category_distribute_percentage = group['event_categories'].value_counts(normalize=True)
            percentage_val = effectual_event_percentage(event_category_distribute_percentage)

        # -. record features
        features_rows.append(
            {
                'dock_id': dock_id,
                'month': month,
                'avg_event_count_per_mmsi': _month_dock_trust_score,
                'departure_count': departure_event_count,
                'arrival_count': arrival_event_count,
                'low_speed_stop_event_percentage': percentage_val
            }
        )
    features_df = pd.DataFrame(features_rows)
    return features_df


def process_middle_features_quarter(
    dock_id: int,
    events_df: pd.DataFrame,
    year: int,
    var: VariablesManager,
    skip_event_count: int = 10
):
    """
    calculate middle features for trust score calculation

    :param dock_id: dock id
    :param events_df: events data frame
    :param year: year
    :param var: VariablesManager
    :param skip_event_count: skip event count
    :return:
    """
    features_rows: list = []
    for quarter in range(1, 4+1):
        _quarter_dock_events = events_df.loc[events_df['quarter'] == quarter]
        _quarter_dock_events.loc[:, 'stop_duration'] = _quarter_dock_events.apply(
            lambda _row: _row['end_time'] - _row['begin_time'], axis=1)
        _quarter_dock_events = _quarter_dock_events.loc[
            (_quarter_dock_events['stop_duration'] > 1800) & (_quarter_dock_events['stop_duration'] < 7 * 24 * 3600)]

        if _quarter_dock_events.shape[0] < skip_event_count:
            # -. record features
            features_rows.append(
                {
                    'dock_id': dock_id,
                    'quarter': quarter,
                    'avg_event_count_per_mmsi': None,
                    'departure_count': None,
                    'arrival_count': None,
                    'low_speed_stop_event_percentage': None
                }
            )
            _logger.info(f"dock_id: {dock_id}, quarter: {quarter} event count is less than {skip_event_count}, skip.")
            continue

        # -. dbscan. to find effectual area
        coords = _quarter_dock_events[[var.column_names.lng_column_name, var.column_names.lat_column_name]].values
        db = DBSCAN(
            eps=0.1 / var.geo_param.kms_per_radian,  # convert 0.1m to radian.
            min_samples=60,
            algorithm='ball_tree',
            metric='haversine'
        ).fit(np.radians(coords))
        _quarter_dock_events.loc[:, 'cluster_id'] = db.labels_

        # -. calculate trust score (event_count_per_mmsi)
        _quarter_dock_trust_score = trust_score(_quarter_dock_events)

        # -. calculate departure&arrival ship count
        departure_event_count, arrival_event_count = calculate_dd_event_count_quarter(
            year=year,
            quarter=quarter,
            dock_id=dock_id,
            con=var.engine
        )

        # -. calculate low_speed / stop_event
        event_category_distribute_percentage = _quarter_dock_events['event_categories'].value_counts(normalize=True)
        percentage_val = effectual_event_percentage(event_category_distribute_percentage)

        # -. calculate low_speed / stop_event in dbscan cluster
        for cluster_id, group in _quarter_dock_events.groupby('cluster_id'):
            event_category_distribute_percentage = group['event_categories'].value_counts(normalize=True)
            percentage_val = effectual_event_percentage(event_category_distribute_percentage)

        # -. record features
        features_rows.append(
            {
                'dock_id': dock_id,
                'quarter': quarter,
                'avg_event_count_per_mmsi': _quarter_dock_trust_score,
                'departure_count': departure_event_count,
                'arrival_count': arrival_event_count,
                'low_speed_stop_event_percentage': percentage_val
            }
        )
    features_df = pd.DataFrame(features_rows)
    return features_df


def run_app():
    parser = argparse.ArgumentParser(description='process middle data for trust score calculation')
    parser.add_argument('--year', type=int, required=True, help='Process year')

    args = parser.parse_args()

    year = args.year

    var = VariablesManager()

    # -. load events
    events_with_dock_df = load_events_with_dock(year=year, con=var.engine)

    # -. clean up events
    events_with_dock_df = clean_up_events(events_with_dock_df, var)

    # -. load dock polygon
    dock_polygon_df = load_dock_polygon(var.engine)
    coal_dock_polygon_df = dock_polygon_df[dock_polygon_df['type_id'] == 1]
    coal_dock_events_df = events_with_dock_df.loc[
        events_with_dock_df['coal_dock_id'].isin(coal_dock_polygon_df['dock_id'])
    ]

    # -. calculate middle features
    feat_data_month_list = []
    feat_data_quarter_list = []
    for _, dock_row in coal_dock_polygon_df.iterrows():
        dock_id = dock_row['dock_id']
        dock_name = dock_row['name']
        _dock_events_df = coal_dock_events_df.loc[coal_dock_events_df['coal_dock_id'] == dock_id]

        features_in_month = process_middle_features_month(
            dock_id=dock_id,
            events_df=_dock_events_df.copy(),
            year=year,
            var=var
        )
        feat_data_month_list.append(features_in_month)
        features_in_quarter = process_middle_features_quarter(
            dock_id=dock_id,
            events_df=_dock_events_df.copy(),
            year=year,
            var=var
        )
        feat_data_quarter_list.append(features_in_quarter)
        _logger.info(f"dock name: {dock_name}, dock_id: {dock_id} features calculation done.")

    feat_month_df = pd.concat(feat_data_month_list)
    feat_quarter_df = pd.concat(feat_data_quarter_list)
    feat_month_df.to_sql(
        name=var.table_names.data_features_trust_score_month_table_name,
        con=var.engine,
        if_exists='replace',
        index=False
    )
    feat_quarter_df.to_sql(
        name=var.table_names.data_features_trust_score_quarter_table_name,
        con=var.engine,
        if_exists='replace',
        index=False
    )


if __name__ == '__main__':
    run_app()

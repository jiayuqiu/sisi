# -*- encoding: utf-8 -*-
'''
@File    :   main_upload_events.py
@Time    :   2025/02/22 00:42:51
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''


import os
import argparse
import pandas as pd

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager
from sisi_ops.infrastructure.definition.parameters import ArgsDefinition as Ad
from sisi_ops.infrastructure.definition.mapping import EVENT_FIELDS_MAPPING
from sisi_ops.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app() -> None:
    parser = argparse.ArgumentParser(description='process: upload events')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    parser.add_argument(f'--{Ad.year}', type=int, required=True, help='Process year')
    parser.add_argument(f'--{Ad.start_month}', type=int, required=True, help='The start month')
    parser.add_argument(f'--{Ad.end_month}', type=int, required=True, help='The end month')

    args = parser.parse_args()

    stage_env = args.__getattribute__(Ad.stage_env)
    year = args.__getattribute__(Ad.year)
    start_month = args.__getattribute__(Ad.start_month)
    end_month = args.__getattribute__(Ad.end_month)
    # months = [f"{year}{x:02}" for x in range(start_month, end_month+1)]

    vars = ShoreNetVariablesManager(stage_env)

    for month in range(start_month, end_month+1):
        month_str = f"{year}{month:02}"
        _logger.info(f"{month_str} events processing...")
        events_df = pd.read_csv(os.path.join(
            vars.dp_names.data_path, stage_env, 'events', f"{month_str}.csv"
        ))
        events_df.dropna(subset=['mmsi'], inplace=True)

        # -. map fields
        events_df.rename(columns=EVENT_FIELDS_MAPPING, inplace=True)
        _logger.info(f"original events shape: {events_df.shape}")

        # -. add year, month, day
        events_begin_time_dt = pd.to_datetime(events_df['begin_time'], unit='s')
        events_df['mmsi'] = events_df['mmsi'].astype(int)
        events_df['begin_year'] = events_begin_time_dt.dt.year
        events_df['begin_month'] = events_begin_time_dt.dt.month
        events_df['begin_day'] = events_begin_time_dt.dt.day

        # # -. get events only in china
        # events_df.loc[:, 'begin_lng'] = events_df['begin_lng'].multiply(0.000001)
        # events_df.loc[:, 'begin_lat'] = events_df['begin_lat'].multiply(0.000001)

        events_df = events_df.loc[(events_df['begin_lng'] > vars.event_param.event_lng_range[0]) &
                                  (events_df['begin_lng'] < vars.event_param.event_lng_range[1]) &
                                  (events_df['begin_lat'] > vars.event_param.event_lat_range[0]) &
                                  (events_df['begin_lat'] < vars.event_param.event_lat_range[1]) &
                                  (events_df['avg_speed'] < vars.event_param.event_avg_speed_max)]

        _logger.info(f"cleaned events shape: {events_df.shape}")

        # -. delete particular year-month data
        vars.engine.execute(
            f"""
            DELETE FROM 
                {vars.warehouse_schema}.factor_all_stop_events
            WHERE 
                begin_year = {year} AND begin_month = {month}
            """
        )
        _logger.info(f"DELETE month: {month_str} events FINISHED.")
        _logger.info(f"INSERT month: {month_str} count: {events_df.shape[0]} events START.")
        events_df.drop_duplicates(subset='event_id', keep='first', inplace=True)
        events_df.to_sql("factor_all_stop_events", con=vars.engine, if_exists='append', index=False)
        _logger.info(f"upload events to db success, count: {events_df.shape[0]}")


if __name__ == '__main__':
    run_app()

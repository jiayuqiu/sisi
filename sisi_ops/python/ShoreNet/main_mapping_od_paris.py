"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  main_mapping_od_paris.py
@DateTime:  16/11/2024 1:24 pm
@DESC    :  the entry to map od pairs. only for year wise.
"""

import os
import argparse

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager
from sisi_ops.ShoreNet.events.generic.tools import load_events_with_dock
from sisi_ops.ShoreNet.events.filter import clean_up_events
from sisi_ops.ShoreNet.analyze.departure_arrival_docks import map_dock_pairs
from sisi_ops.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app():
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument('--stage_env', type=str, required=True, help='Stage environment')
    parser.add_argument('--years', type=str, required=True, help='Process years, please use comma to split the year. e.g, 2022,2023')

    args = parser.parse_args()

    stage_env = args.stage_env
    years = args.years
    year_list = [_year for _year in years.split(",")]

    vars = ShoreNetVariablesManager(stage_env)
    # -. load events
    events_with_dock_df = load_events_with_dock(
        year_list=year_list, 
        vars=vars
    )

    # -. clean up events
    events_with_dock_df = clean_up_events(events_with_dock_df, vars)

    # -. map od pairs
    events_od_df = map_dock_pairs(events_with_dock_df, vars.process_workers)

    # -. write od pairs, to database and csv
    events_od_df.to_csv(
        path_or_buf=os.path.join(
            vars.dp_names.output_path, 'csv', f"{years}_od_pairs.csv"
        ),
        index=False
    )

    events_od_df.to_sql(
        name=vars.table_names.data_od_pairs,
        con=vars.engine,
        if_exists='replace',
        index=False
    )


if __name__ == '__main__':
    run_app()

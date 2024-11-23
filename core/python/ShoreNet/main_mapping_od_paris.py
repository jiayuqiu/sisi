"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  main_mapping_od_paris.py
@DateTime:  16/11/2024 1:24 pm
@DESC    :  the entry to map od pairs. only for year wise.
"""

import os
import argparse

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.events.generic.tools import load_events_with_dock
from core.ShoreNet.events.filter import clean_up_events
from core.ShoreNet.analyze.departure_arrival_docks import map_dock_pairs
from core.ShoreNet.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app():
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument('--year', type=int, required=True, help='Process year')

    args = parser.parse_args()

    year = args.year

    var = VariablesManager()
    # -. load events
    events_with_dock_df = load_events_with_dock(year=year, con=var.engine)

    # -. clean up events
    events_with_dock_df = clean_up_events(events_with_dock_df, var)

    # -. map od pairs
    events_od_df = map_dock_pairs(events_with_dock_df, var.process_workers)

    # -. write od pairs, to database and csv
    events_od_df.to_csv(
        path_or_buf=os.path.join(
            var.data_path, var.dp_names.output_path, 'csv', f"{year}_od_pairs.csv"
        ),
        index=False
    )

    events_od_df.to_sql(
        name=var.table_names.data_od_pairs_table_name,
        con=var.engine,
        if_exists='replace',
        index=False
    )


if __name__ == '__main__':
    run_app()

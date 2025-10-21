"""
@Author  :  Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  main_map_events_polygons.py
@DateTime:  27/10/2024 3:57 pm
@DESC    :  match polygon for events which is without polygon
"""

import argparse
import traceback

import numpy as np
from numba.typed import List
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager
from sisi_ops.infrastructure.definition.parameters import (
    ArgsDefinition as Ad,
    ColumnNames as Cn
)
from sisi_ops.ShoreNet.utils.db.FactorAllStopEvent import FactorAllStopEvents
from sisi_ops.ShoreNet.events.generic.tools import load_events_month, load_dock_polygon, load_csv_dock_polygon
from sisi_ops.ShoreNet.events.polygon import match_polygons
from sisi_ops.utils.setup_logger import set_logger
from sisi_ops.utils.helper.tools import flag_str2bool

_logger = set_logger(__name__)


def factor_update_executor(vars: ShoreNetVariablesManager, update_items: list, bulk_size: int = 5000):
    Session = sessionmaker(bind=vars.engine)
    session = Session()
    try:
        for start in tqdm(range(0, len(update_items), bulk_size), desc=f"Updating coal_dock_id"):
            chunk_items = update_items[start:(start+bulk_size)]
            session.bulk_update_mappings(FactorAllStopEvents, chunk_items)
            session.commit()
    except Exception as e:
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()


def run_app() -> None:
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    parser.add_argument(f'--{Ad.year}', type=int, required=True, help='Process year')
    parser.add_argument(f'--{Ad.start_month}', type=int, required=True, help='The start month')
    parser.add_argument(f'--{Ad.end_month}', type=int, required=True, help='The end month')
    parser.add_argument(f'--{Ad.polygon_fn}', type=str, required=True, help='The end month')
    parser.add_argument(f"--{Ad.reset_flag}", type=flag_str2bool, 
                        help='If reset coal_dock_id as null before map.',
                        nargs="?",
                        const=False,
                        default=False)

    args = parser.parse_args()

    stage_env = args.__getattribute__(Ad.stage_env)
    year = args.__getattribute__(Ad.year)
    start_month = args.__getattribute__(Ad.start_month)
    end_month = args.__getattribute__(Ad.end_month)
    polygon_fn = args.__getattribute__(Ad.polygon_fn)
    reset_flag = args.__getattribute__(Ad.reset_flag)

    vars = ShoreNetVariablesManager(stage_env)

    for month in range(start_month, end_month+1):
        month_str = f"{year}{month:02}"
        _logger.info(f"{month_str} events polygon pairing processing...")

        if reset_flag:
            # reset coal_dock_id
            vars.engine.execute(
                f"update sisi_dev.factor_all_stop_events t set t.coal_dock_id = null where t.begin_year = {year} and t.begin_month = {month} and t.coal_dock_id is not null;"
            )
            _logger.warning(
                f"year: {year}, month: {month} data have been reset."
            )
            # exit(1)

        # -. load events without polygon
        events_df = load_events_month(
            year=year,
            month=month,
            vars=vars
        )
        _logger.info(f"original events shape: {events_df.shape}")

        # -. load polygon data
        # dock_polygon_list = load_dock_polygon(vars)
        dock_polygon_list = load_csv_dock_polygon(polygon_fn)

        numba_polygons = List()
        for poly in dock_polygon_list:
            numba_polygons.append(np.array(poly["polygon"], dtype=np.float64))
        _logger.info(f"dock polygon count: {len(dock_polygon_list)}")

        # -. match polygon
        points = events_df[[Cn.lng, Cn.lat]].to_numpy(dtype=np.float64)
        matched_index_list = match_polygons(points, numba_polygons)
        dock_ids = [dock_polygon_list[i]['dock_id'] if i != -1 else None for i in matched_index_list]
        events_df.loc[:, 'coal_dock_id'] = dock_ids
        events_df = events_df.loc[(events_df['coal_dock_id'] != -1) & (events_df['coal_dock_id'].notnull())]

        _logger.info(f"matched events shape: {events_df.shape}")
        if events_df.shape[0] == 0:
            _logger.info(f"{month_str} didn't pair any polygons. ")
            continue

        # -. update dock id to db
        update_items = []
        for _, row in events_df.iterrows():
            coal_dock_id_val = None
            if row['coal_dock_id'] is not None and not np.isnan(row['coal_dock_id']):
                coal_dock_id_val = int(row['coal_dock_id'])

            update_items.append({
                "event_id": row["event_id"],
                "coal_dock_id": coal_dock_id_val,
            })

        factor_update_executor(vars, update_items)


if __name__ == '__main__':
    run_app()

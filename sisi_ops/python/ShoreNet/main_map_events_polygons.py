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

_logger = set_logger(__name__)


def run_app() -> None:
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    parser.add_argument(f'--{Ad.year}', type=int, required=True, help='Process year')
    parser.add_argument(f'--{Ad.start_month}', type=int, required=True, help='The start month')
    parser.add_argument(f'--{Ad.end_month}', type=int, required=True, help='The end month')

    args = parser.parse_args()

    stage_env = args.__getattribute__(Ad.stage_env)
    year = args.__getattribute__(Ad.year)
    start_month = args.__getattribute__(Ad.start_month)
    end_month = args.__getattribute__(Ad.end_month)

    vars = ShoreNetVariablesManager(stage_env)

    for month in range(start_month, end_month+1):
        month_str = f"{year}{month:02}"
        _logger.info(f"{month_str} events polygon pairing processing...")

        # -. load events without polygon
        events_df = load_events_month(
            year=year,
            month=month,
            vars=vars
        )
        _logger.info(f"original events shape: {events_df.shape}")

        # -. load polygon data
        # dock_polygon_list = load_dock_polygon(vars)
        dock_polygon_list = load_csv_dock_polygon("/mnt/smbfn/data/sisi/dock/coal_docks_polygon.csv")

        numba_polygons = List()
        for poly in dock_polygon_list:
            numba_polygons.append(np.array(poly["polygon"], dtype=np.float64))
        _logger.info(f"dock polygon count: {len(dock_polygon_list)}")

        # -. match polygon
        points = events_df[[Cn.lng, Cn.lat]].to_numpy(dtype=np.float64)
        matched_index_list = match_polygons(points, numba_polygons)
        dock_ids = [dock_polygon_list[i]['dock_id'] if i != -1 else None for i in matched_index_list]
        events_df.loc[:, 'coal_dock_id'] = dock_ids
        events_df = events_df.loc[events_df['coal_dock_id'] != -1]

        _logger.info(f"matched events shape: {events_df.shape}")
        if events_df.shape[0] == 0:
            _logger.info(f"{month_str} didn't pair any polygons. ")
            continue

        # -. update dock id to db
        Session = sessionmaker(bind=vars.engine)
        session = Session()
        try:
            # Perform the update within a transaction
            for _, row in tqdm(events_df.iterrows(), total=events_df.shape[0], desc=f"Updating Events {month_str}"):
                if row['coal_dock_id'] is None:
                    coal_dock_id_val = None
                elif np.isnan(row['coal_dock_id']):
                    coal_dock_id_val = None
                else:
                    coal_dock_id_val = int(row['coal_dock_id'])
                session.query(FactorAllStopEvents).filter(
                    FactorAllStopEvents.event_id == row['event_id']
                ).update({
                    FactorAllStopEvents.coal_dock_id: coal_dock_id_val
                }, synchronize_session=False)

            # Commit the transaction
            session.commit()
        except Exception as e:
            # Rollback the transaction if an exception occurs
            traceback.print_exc()
            session.rollback()
            _logger.error(f"Update failed: {e}")
        finally:
            # Close the session
            session.close()


if __name__ == '__main__':
    run_app()

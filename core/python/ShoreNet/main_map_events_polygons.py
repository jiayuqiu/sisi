"""
@Author  :  Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  main_map_events_polygons.py
@DateTime:  27/10/2024 3:57 pm
@DESC    :  match polygon for events which is without polygon
"""

import time
import argparse
import traceback

import numpy as np
from sqlalchemy.orm import sessionmaker
from numba import cuda, njit

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.infrastructure.definition.parameters import (
    ArgsDefinition as Ad,
    ColumnNames as Cn
)
from core.ShoreNet.utils.db.FactorAllStopEvent import FactorAllStopEvents
from core.ShoreNet.events.generic.tools import load_events_all, load_dock_polygon
from core.ShoreNet.events.polygon import map_event_polygon
from core.ShoreNet.utils.geo import point_poly_cuda
from core.utils.setup_logger import set_logger

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
        events_df = load_events_all(
            year=year,
            month=month,
            vars=vars
        )
        _logger.info(f"original events shape: {events_df.shape}")

        # -. load coal mmsi statics and get coal events
        # events_df = events_df
        _logger.info(f"coal events shape: {events_df.shape}")

        # -. load polygon data
        dock_polygon_list = load_dock_polygon(vars)
        _logger.info(f"dock polygon count: {len(dock_polygon_list)}")

        # -. match polygon
        from pandarallel import pandarallel
        pandarallel.initialize(progress_bar=True, nb_workers=vars.process_workers)
        start_t = time.time()
        dock_tag = events_df.parallel_apply(
            map_event_polygon, args=(dock_polygon_list,), axis=1
        )
        end_t = time.time()
        _logger.info(f"CPU matching time: {end_t - start_t}")

        events_df.loc[:, 'coal_dock_id'] = dock_tag
        events_df = events_df.loc[events_df['coal_dock_id'].notnull()]
        _logger.info(f"matched events shape: {events_df.shape}")

        if events_df.shape[0] == 0:
            _logger.info(f"{month_str} didn't pair any polygons. ")
            continue

        # -. update dock id to db
        Session = sessionmaker(bind=vars.engine)
        session = Session()
        try:
            # Perform the update within a transaction
            for _, row in events_df.iterrows():
                # query = f"""
                # UPDATE {Prefix.sisi}{stage_env}.factor_all_stop_events
                # SET coal_dock_id = {int(row['coal_dock_id'])}
                # WHERE event_id = '{row['event_id']}'
                # """
                # session.execute(query)

                session.query(FactorAllStopEvents).filter(
                    FactorAllStopEvents.event_id == row['event_id']
                ).update({
                    FactorAllStopEvents.coal_dock_id: int(row['coal_dock_id'])
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

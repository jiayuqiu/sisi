"""
@Author  :  Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  main_map_events_polygons.py
@DateTime:  27/10/2024 3:57 pm
@DESC    :  match polygon for events which is without polygon
"""

import numpy as np
from numba.typed import List
from numba import njit, prange

from sisi_ops.ShoreNet.events.polygon import map_event_polygon, point_in_poly
# from core.ShoreNet.utils.geo import point_poly_cuda
from sisi_ops.utils.setup_logger import set_logger

_logger = set_logger(__name__)


@njit(parallel=True)
def match_polygons(points, polygons):
    n_points = points.shape[0]
    result = -np.ones(n_points, dtype=np.int64)
    for i in prange(n_points):
        lng = points[i, 0]
        lat = points[i, 1]
        for j in range(len(polygons)):
            poly = polygons[j]
            if point_in_poly(lat, lng, poly):
                result[i] = j  # here polygon index is used as the dock tag
                break
    return result


def run_app() -> None:
    pass
    # TODO: after get 2023 ais data, develop this entry
    # parser = argparse.ArgumentParser(description='process match polygon for events')
    # parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    # parser.add_argument(f'--{Ad.year}', type=int, required=True, help='Process year')
    # parser.add_argument(f'--{Ad.start_month}', type=int, required=True, help='The start month')
    # parser.add_argument(f'--{Ad.end_month}', type=int, required=True, help='The end month')
    #
    # args = parser.parse_args()
    #
    # stage_env = args.__getattribute__(Ad.stage_env)
    # year = args.__getattribute__(Ad.year)
    # start_month = args.__getattribute__(Ad.start_month)
    # end_month = args.__getattribute__(Ad.end_month)
    #
    # vars = ShoreNetVariablesManager(stage_env)
    #
    # for month in range(start_month, end_month+1):
    #     month_str = f"{year}{month:02}"
    #     _logger.info(f"{month_str} events polygon pairing processing...")
    #
    #     # -. load events without polygon
    #     events_df = load_events_all(
    #         year=year,
    #         month=month,
    #         vars=vars
    #     )
    #     _logger.info(f"original events shape: {events_df.shape}")
    #
    #     # -. load coal mmsi statics and get coal events
    #     # events_df = events_df
    #     _logger.info(f"coal events shape: {events_df.shape}")
    #
    #     # -. load polygon data
    #     dock_polygon_list = load_dock_polygon(vars)
    #     numba_polygons = List()
    #     for poly in dock_polygon_list:
    #         numba_polygons.append(np.array(poly["polygon"], dtype=np.float64))
    #     _logger.info(f"dock polygon count: {len(dock_polygon_list)}")
    #
    #     # -. match polygon
    #     points = events_df[[Cn.lng, Cn.lat]].to_numpy(dtype=np.float64)
    #     dock_ids = match_polygons(points, numba_polygons)
    #     events_df.loc[:, 'coal_dock_id'] = dock_ids
    #     events_df = events_df.loc[events_df['coal_dock_id'] != -1]
    #
    #     _logger.info(f"matched events shape: {events_df.shape}")
    #     if events_df.shape[0] == 0:
    #         _logger.info(f"{month_str} didn't pair any polygons. ")
    #         continue
    #
    #     # -. update dock id to db
    #     Session = sessionmaker(bind=vars.engine)
    #     session = Session()
    #     try:
    #         # Perform the update within a transaction
    #         for _, row in events_df.iterrows():
    #             session.query(FactorAllStopEvents).filter(
    #                 FactorAllStopEvents.event_id == row['event_id']
    #             ).update({
    #                 FactorAllStopEvents.coal_dock_id: int(row['coal_dock_id'])
    #             }, synchronize_session=False)
    #
    #         # Commit the transaction
    #         session.commit()
    #     except Exception as e:
    #         # Rollback the transaction if an exception occurs
    #         traceback.print_exc()
    #         session.rollback()
    #         _logger.error(f"Update failed: {e}")
    #     finally:
    #         # Close the session
    #         session.close()


if __name__ == '__main__':
    run_app()

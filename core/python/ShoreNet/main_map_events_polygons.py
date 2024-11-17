"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  main_map_events_polygons.py
@DateTime:  27/10/2024 3:57 pm
@DESC    :  match polygon for events which is without polygon
"""

import argparse
import traceback

from sqlalchemy.orm import sessionmaker

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.events.generic.tools import load_events_all, load_dock_polygon
from core.ShoreNet.statics.generic.tools import load_coal_ship_statics
from core.ShoreNet.events.dock import map_event_polygon
from core.ShoreNet.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app() -> None:
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument('--year', type=int, required=True, help='Process year')
    parser.add_argument('--start_month', type=int, required=True, help='The start month')
    parser.add_argument('--end_month', type=int, required=True, help='The end month')

    args = parser.parse_args()

    year = args.year
    start_month = args.start_month
    end_month = args.end_month
    # months = [f"2023{x:02}" for x in range(start_month, end_month+1)]

    var = VariablesManager()

    for month in range(start_month, end_month+1):
        month_str = f"{year}{month:02}"
        _logger.info(f"{month_str} events polygon pairing processing...")

        # -. load events without polygon
        events_df = load_events_all(
            year=year,
            month=month,
            con=var.engine
        )
        _logger.info(f"original events shape: {events_df.shape}")

        # -. load coal mmsi statics and get coal events
        coal_events_df = events_df
        _logger.info(f"coal events shape: {coal_events_df.shape}")

        # -. load polygon data
        dock_polygon_list = load_dock_polygon(con=var.engine)
        _logger.info(f"dock polygon count: {len(dock_polygon_list)}")

        # -. match polygon
        from pandarallel import pandarallel
        pandarallel.initialize(progress_bar=False, nb_workers=var.process_workers)
        dock_tag = coal_events_df.parallel_apply(
            map_event_polygon, args=(dock_polygon_list,), axis=1
        )

        coal_events_df.loc[:, 'coal_dock_id'] = dock_tag
        coal_events_df = coal_events_df.loc[coal_events_df['coal_dock_id'].notnull()]
        if coal_events_df.shape[0] == 0:
            _logger.info(f"{month_str} didn't pair any polygons. ")
            return None

        # -. update dock id to db
        Session = sessionmaker(bind=var.engine)
        session = Session()
        try:
            # Perform the update within a transaction
            for _, row in coal_events_df.iterrows():
                query = f"""
                UPDATE sisi.factor_all_stop_events
                SET coal_dock_id = {int(row['coal_dock_id'])}
                WHERE event_id = '{row['event_id']}'
                """
                session.execute(query)

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

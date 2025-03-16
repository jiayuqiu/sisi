# -*- encoding: utf-8 -*-
'''
@File    :   uploader.py
@Time    :   2025/02/22 00:55:33
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   This script is used to define the uploader class which is used to upload following data to the database:
             - events
             - dock polygons
             - static data
'''

import os
import glob

from sqlalchemy.orm import sessionmaker
from geoalchemy2 import WKTElement
import pandas as pd

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.infrastructure.definition.parameters import (
    Prefix,
    ColumnNames as Cn
)
from core.ShoreNet.utils.polygon import KMLParser
from core.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon
from core.ShoreNet.definitions.mapping import EVENT_FIELDS_MAPPING
from core.ShoreNet.statics.filter import clean_up_statics
# from core.ShoreNet.definitions.parameters import ArgsDefinition as Ad
from core.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def insert_polygon(vars: ShoreNetVariablesManager, parsed_kp_ls: list) -> None:
    """insert parsed dock polygon data into the database

    Args:
        vars (ShoreNetVariablesManager): variables manager.
        parsed_kp_ls (list): A list of parsed dock polygon data dictionaries.
            Each dictionary in the list represents a dock polygon and has the following keys:
            - 'name' (str): The name of the dock polygon.
            - 'polygon' (list): A list of CoordinatePoint objects that define the vertices of the polygon.
            - 'province' (str): The name of the province the dock is located in.
    """
    Session = sessionmaker(bind=vars.engine)
    session = Session()

    for parsed_kp in parsed_kp_ls:
        polygon_wkt = f"POLYGON(({', '.join([f'{coord.lat} {coord.lng}' for coord in parsed_kp['polygon']])}))"

        # Create a DimDockPolygon instance
        new_dock = DimDockPolygon(
            Name=parsed_kp['name'],
            Polygon=WKTElement(polygon_wkt, srid=4326),  # srid=4326 is commonly used for GPS coordinates
            lng=parsed_kp['polygon'][0].lng,
            lat=parsed_kp['polygon'][0].lat,
            type_id=None,
            stage_id=None
        )

        # Add and commit the new dock entry
        session.add(new_dock)
    
    # Commit all the changes
    session.commit()

    # Close the session
    session.close()
    
    _logger.info("Dock polygon data inserted successfully.")


class DataUploader:
    def __init__(self,
                 stage_env: str,
                 vars: ShoreNetVariablesManager,
                 year: int = None,
                 start_month: int = None,
                 end_month: int = None):
        self.stage_env = stage_env
        self.vars = vars
        self.year = year
        self.start_month = start_month
        self.end_month = end_month
        self.warehouse_schema = f"{Prefix.sisi}{self.stage_env}"
    
    def upload_events(self, ):
        for month in range(self.start_month, self.end_month+1):
            month_str = f"{self.year}{month:02}"
            _logger.info(f"{month_str} events processing...")
            events_df = pd.read_csv(os.path.join(
                self.vars.dp_names.data_path, self.stage_env, 'events', f"{month_str}.csv"
            ))
            events_df.dropna(subset=[Cn.mmsi], inplace=True)

            # -. map fields
            events_df.rename(columns=EVENT_FIELDS_MAPPING, inplace=True)
            _logger.debug(f"original events shape: {events_df.shape}")

            # -. add year, month, day
            events_begin_time_dt = pd.to_datetime(events_df['begin_time'], unit='s')
            events_df[Cn.mmsi] = events_df[Cn.mmsi].astype(int)
            events_df[Cn.year] = events_begin_time_dt.dt.year
            events_df[Cn.month] = events_begin_time_dt.dt.month
            events_df[Cn.day] = events_begin_time_dt.dt.day

            # # -. get events only in china
            # events_df.loc[:, 'begin_lng'] = events_df['begin_lng'].multiply(0.000001)
            # events_df.loc[:, 'begin_lat'] = events_df['begin_lat'].multiply(0.000001)

            events_df = events_df.loc[(events_df[Cn.lng] > self.vars.event_param.event_lng_range[0]) &
                                      (events_df[Cn.lng] < self.vars.event_param.event_lng_range[1]) &
                                      (events_df[Cn.lat] > self.vars.event_param.event_lat_range[0]) &
                                      (events_df[Cn.lat] < self.vars.event_param.event_lat_range[1]) &
                                      (events_df[Cn.sog] < self.vars.event_param.event_avg_speed_max)]

            _logger.debug(f"cleaned events shape: {events_df.shape}")

            # -. delete particular year-month data
            self.vars.engine.execute(
                f"""
                DELETE FROM 
                    {self.warehouse_schema}{self.stage_env}.factor_all_stop_events
                WHERE 
                    begin_year = {self.year} AND begin_month = {month}
                """
            )
            _logger.debug(f"DELETE month: {month_str} events FINISHED.")
            _logger.info(f"INSERT month: {month_str} count: {events_df.shape[0]} events START.")
            events_df.drop_duplicates(subset='event_id', keep='first', inplace=True)
            events_df.to_sql("factor_all_stop_events", con=self.vars.engine, if_exists='append', index=False)
            _logger.info(f"upload events to db success, count: {events_df.shape[0]}")

    def upload_dock_polygons(self, ):
        # get all kml files by `glob`
        kml_fn_ls = glob.glob(os.path.join(self.vars.dp_names.data_path, self.stage_env, "kml", '*.kml'))
        
        parsed_kml_kp_ls = []
        for kml_fn in kml_fn_ls:
            parsed_kml_ls = KMLParser(kml_fn, self.vars).parse_kml()
            for parsed_kml in parsed_kml_ls:
                parsed_kml_kp_ls.append(parsed_kml)
        
        # insert polygon data into database
        insert_polygon(self.vars, parsed_kml_kp_ls)

    def upload_static_data(self, ):
        # -. get DWT
        ship_dwt_df = pd.read_csv(
            os.path.join(
                self.vars.dp_names.data_path,
                self.stage_env, "statics",
                self.vars.f_names.ship_dwt_fn
            )
        )
        ship_dwt_df.loc[:, 'DWT_float'] = ship_dwt_df['DWT'].str.replace(',', '').astype(float)
        ship_dwt_df = ship_dwt_df[ship_dwt_df['DWT_float'] > 0]

        # -. get statics
        ship_statics_df = pd.read_csv(
            os.path.join(self.vars.dp_names.data_path, self.stage_env, "statics", self.vars.f_names.ship_statics_fn)
        )
        _logger.debug(f"static data shape before cleaning: {ship_statics_df.shape}")
        ship_statics_df = ship_statics_df.rename(
            columns={
                "shipname": "ship_name",
                "shiptype": "ship_type",
                "breadth": "width",
                "DWT_float": "dwt"
            }
        )

        # -. clean statics
        cleaned_statics_df = clean_up_statics(ship_statics_df)
        _logger.debug(f"static data shape after cleaning: {cleaned_statics_df.shape}")

        # -. merge with dwt
        cleaned_statics_df = pd.merge(cleaned_statics_df, ship_dwt_df.loc[:, ["mmsi", "DWT_float"]], on=Cn.mmsi, how='left')

        # -. upload statics to database
        _logger.info("uploading statics data to database...")
        # cleaned_statics_df = cleaned_statics_df.loc[:, ["mmsi", "ship_name", "ship_type", "length", "breadth", "DWT_float"]]
        # cleaned_statics_df = cleaned_statics_df.rename(
        #     columns={
        #         "shipname": "ship_name",
        #         "shiptype": "ship_type",
        #         "breadth": "width",
        #         "DWT_float": "dwt"
        #     }
        # )
        cleaned_statics_df = cleaned_statics_df.drop_duplicates(subset=[Cn.mmsi], keep="first")
        cleaned_statics_df.to_sql("dim_ships_statics", con=self.vars.engine, if_exists='replace', index=False)
        _logger.info("uploaded statics data to database successfully")
    
    def _clean_all_data(self, ):
        """Only for dummy stage, clean all data in the database
        """
        self.vars.engine.execute(
            f"""
            DELETE FROM 
                {self.warehouse_schema}{self.stage_env}.factor_all_stop_events
            """
        )
        self.vars.engine.execute(
            f"""
            DELETE FROM 
                {self.warehouse_schema}{self.stage_env}.dim_ships_statics
            """
        )
        self.vars.engine.execute(
            f"""
            DELETE FROM 
                {self.warehouse_schema}{self.stage_env}.dim_dock_polygon
            """
        )
        _logger.info("All data cleaned successfully.")

    def upload(self, ):
        pass

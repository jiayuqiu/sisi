# -*- encoding: utf-8 -*-
'''
@File    :   main_upload_polygon.py
@Time    :   2025/02/15 20:23:05
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   This script is to parse the dock polygon data from kml files and insert them into the database
'''

import os
import glob
import argparse

from sqlalchemy.orm import sessionmaker
from geoalchemy2 import WKTElement

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.ShoreNet.definitions.parameters import ArgsDefinition as Ad
from core.ShoreNet.utils.polygon import KMLParser
from core.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon
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


def run_app():
    """Runs the main application logic for parsing KML files and inserting polygon data into the database.
    """
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    args = parser.parse_args()
    stage_env = args.__getattribute__(Ad.stage_env)

    vars = ShoreNetVariablesManager(stage_env)
    
    # get all kml files by `glob`
    kml_fn_ls = glob.glob(os.path.join(vars.dp_names.data_path, stage_env, "kml", '*.kml'))
    
    parsed_kml_kp_ls = []
    for kml_fn in kml_fn_ls:
        parsed_kml_ls = KMLParser(kml_fn, vars).parse_kml()
        for parsed_kml in parsed_kml_ls:
            parsed_kml_kp_ls.append(parsed_kml)
    
    # insert polygon data into database
    insert_polygon(vars, parsed_kml_kp_ls)
    

if __name__ == "__main__":
    run_app()

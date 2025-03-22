# -*- encoding: utf-8 -*-
"""
@File    :   polygons.py
@Time    :   2025/03/22 00:19:22
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
"""


from sqlalchemy.orm import sessionmaker
from geoalchemy2 import WKTElement

from core.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon
from core.ShoreNet.utils.polygon import KMLParser
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
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


class PolygonsDataProcessor(object):
    def __init__(self, kml_fn):
        self.csv_file = kml_fn
        self.kml_parser = KMLParser(kml_file=kml_fn)

        # depends on the structure of the kml file
        # please update the for loop in `parse_kml`
        self.parsed_kml_ls = self.kml_parser.parse_kml()

    def get_polygon_detail(self, ):
        parsed_kml_kp_ls = []
        for parsed_kml in self.parsed_kml_ls:
            parsed_kml_kp_ls.append(parsed_kml)
        return parsed_kml_kp_ls

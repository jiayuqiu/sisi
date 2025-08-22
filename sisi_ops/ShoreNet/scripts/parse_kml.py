import traceback
import os
import platform

import pandas as pd
from pykml import parser
from pprint import pprint
# from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
# from sqlalchemy.orm import declarative_base, Session
# from geoalchemy2 import Geometry
# from geoalchemy2.shape import from_shape
# from shapely.geometry import Polygon

# from core.conf import engine


"""
parse kml file and store into SQL Server by sqlalchemy

invalid - it's not necessary to maintain the dock polygons by SQL Server. By now, data were saved by KML.
"""


# # Define metadata
# Base = declarative_base()
#
#
# # Define the table structure
# class DockData(Base):
#     __tablename__ = 'dim_dock'
#     __table_args__ = {'schema': 'ShoreNet'}  # Adjust the schema name if necessary
#     dock_id = Column(Integer, primary_key=True)
#     dock_name = Column(String(50))
#     dock_polygon = Column(Geometry('POLYGON'))


def get_data_path():
    os_name = platform.system()
    if os.name == 'nt' or os_name == 'Windows':
        data_path = r"D:/data/sisi/"
    elif os.name == 'posix' or os_name == 'Linux':
        data_path = r"/mnt/d/data/sisi/"
    else:
        data_path = "/mnt/d/data/sisi/"
    return data_path


DATA_PATH = get_data_path()
STAGE_ID = 3


def parse_kml_root(kml_fn: str):
    """
    parse kml file, get coordinates from it.

    :param kml_fn: kml file path
    :return: {'name': polygon_name, 'polygon': [[lon1, lat1], [lon2, lat2], [lon3, lat3], [lon4, lat4]]}
    """
    from pykml import parser
    with open(kml_fn, mode='r', encoding='utf-8') as f:
        root = parser.parse(f).getroot()

    # add kml file
    r = []
    for area in root.Document.Folder.Folder:
        area_name = area.name.text.strip()
        for place in area.Placemark:
            place_name = place.name.text.strip()
            coordinates = place.Polygon.outerBoundaryIs.LinearRing.coordinates.text.strip()
            place_points = [[round(float(y), 6) for y in x.split(',')[:2]] for x in coordinates.split(' ')]
            r.append({'name': place_name, 'polygon': place_points, 'province': area_name})

    # add excel file
    data_path = get_data_path()
    df = pd.read_excel(os.path.join(data_path, "dock/煤炭码头多边形标定.xlsx"), usecols=['城市', '码头', '经纬度'])
    for _, row in df.iterrows():
        try:
            place_points = [[round(float(y), 6) for y in x.split(',')[:2]] for x in row['经纬度'].split(' ')]
            r.append({'name': row['码头'], 'polygon': place_points, 'province': row['城市']})
        except:
            print(f"error city: {row['码头']}")
    return r


def parse_kml_document(kml_fn: str):
    """
    parse kml file, get coordinates from it.

    :param kml_fn: kml file path
    :return: {'name': polygon_name, 'polygon': [[lon1, lat1], [lon2, lat2], [lon3, lat3], [lon4, lat4]]}
    """
    from pykml import parser
    with open(kml_fn, mode='r', encoding='utf-8') as f:
        root = parser.parse(f).getroot()

    # add kml file
    r = []
    for area in root.Document.Folder:
        area_name = area.name.text.strip()
        for place in area.Placemark:
            place_name = place.name.text.strip()
            coordinates = place.Polygon.outerBoundaryIs.LinearRing.coordinates.text.strip()
            place_points = [[round(float(y), 6) for y in x.split(',')[:2]] for x in coordinates.split(' ')]
            r.append({'name': place_name, 'polygon': place_points, 'province': area_name})

    return r


# def store_polygon(poly_points):
#     with Session(engine) as session:
#         try:
#             polygon = Polygon(poly_points)
#
#             # Insert data into the table
#             new_dock = DockData(
#                 dock_id=1,
#                 dock_name='QHD NO.1',
#                 dock_polygon=from_shape(polygon, srid=4326)  # SRID is the Spatial Reference System Identifier
#             )
#
#             # Add and commit the new dock data
#             session.add(new_dock)
#             session.commit()
#
#         except Exception as e:
#             # Rollback the session in case of any error
#             session.rollback()
#             print(f"An error occurred: {e}")
#             traceback.print_exc()
#
#         finally:
#             # Close the session
#             session.close()


if __name__ == '__main__':
    os_name = platform.system()
    if os.name == 'nt' or os_name == 'Windows':
        DATA_PATH = r"D:/data/sisi/"
    elif os.name == 'posix' or os_name == 'Linux':
        DATA_PATH = r"/mnt/d/data/sisi/"
    else:
        DATA_PATH = "/mnt/d/data/sisi/"

    file_name = os.path.join(DATA_PATH, 'dock/docks_stage_2.kml')
    test_poly_points = parse_kml(file_name)
    pprint(test_poly_points)
    # store_polygon(test_poly_points)

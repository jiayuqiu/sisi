# -*- encoding: utf-8 -*-
'''
@File    :   polygon.py
@Time    :   2024/07/25 20:21:04
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

import os

import pandas as pd
from pykml import parser

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
# import pymssql
# import time

# from scipy.spatial import ConvexHull, QhullError


class CoordinatePoint(object):
    def __init__(self, lat: float, lng: float) -> None:
        self.lat = lat
        self.lng = lng
        self.__validate()
    
    def __validate(self) -> None:
        if not (-90 <= self.lat <= 90):
            raise ValueError(f"Latitude {self.lat} is out of range")
        if not (-180 <= self.lng <= 180):
            raise ValueError(f"Longitude {self.lng} is out of range")

    def __str__(self):
        return f"Latitude: {self.lat}, Longitude: {self.lng}"

    def __eq__(self, other):
        if isinstance(other, CoordinatePoint):
            return self.lat == other.lat and self.lng == other.lng
        return False


class KMLParser(object):
    def __init__(self, kml_file: str, var: ShoreNetVariablesManager) -> None:
        self.kml_file = kml_file
        # self.df = self.parse_kml()

    def parse_kml(self) -> list[dict]:
        """
        Parse KML file and return a DataFrame with latitude and longitude columns

        :return: DataFrame, latitude and longitude columns
        """
        with open(self.kml_file, mode='r', encoding='utf-8') as f:
            root = parser.parse(f).getroot()
        
        # add kml file
        r = []
        """
        NOTE: depends on the way you demarcate the areas in the Google Earth
        `root.Document.Folder.Folder` is the path to the area demarcation, you may need to change it
        """
        for area in root.Document.Folder:
            area_name = area.name.text.strip()
            for place in area.Placemark:
                place_name = place.name.text.strip()
                coordinates = place.Polygon.outerBoundaryIs.LinearRing.coordinates.text.strip()
                place_points = [[round(float(y), 6) for y in x.split(',')[:2]] for x in coordinates.split(' ')]
                coordinate_points = [CoordinatePoint(lat=point[1], lng=point[0]) for point in place_points]
                r.append({'name': place_name, 'polygon': coordinate_points, 'province': area_name})

        return r


# class DockPolygon(object):
#     def __init__(self, dock_id: int, polygon: list[[float, float]]) -> None:
#         self.dock_id = dock_id
#         self.polygon = polygon

#     def __str__(self):
#         return f"Dock ID: {self.dock_id}, Polygon: {self.polygon}"

#     def score(self, events_df: pd.DataFrame) -> float:
#         """
#         Calculate the trust score of the dock based on the events data

#         :param events_df: DataFrame, events data
#         :return: float, trust score of the dock
#         """
#         pass


# # Function to compute convex hull and return the coordinates
# def compute_convex_hull(df):
#     unique_clusters = df['cluster'].unique()
#     hulls = {}
#
#     for cluster in unique_clusters:
#         cluster_points = df[df['cluster'] == cluster][['lng', 'lat']].values
#         if len(cluster_points) > 2:  # Convex hull requires at least 3 points
#             try:
#                 hull = ConvexHull(cluster_points)
#                 hull_points = cluster_points[hull.vertices]
#                 hulls[cluster] = hull_points
#             except QhullError as e:
#                 print(f"QhullError for cluster {cluster}: {e}")
#         else:
#             hulls[cluster] = cluster_points  # For less than 3 points, just use the points themselves
#
#     return hulls


# def hull_points_to_sql_server(df):
#     hulls = compute_convex_hull(df)
#     conn = pymssql.connect(sql_server_properties['host'], sql_server_properties['user'],
#                            'Amacs@0212', sql_server_properties['database'])
#     cursor = conn.cursor()
#
#     # Insert polygon data
#     insert_query = """
#     INSERT INTO ShoreNet.tab_dock_polygon (Name, type_id, lng, lat, Polygon)
#     VALUES (N'%s', %d, %f, %f, geometry::STGeomFromText('%s', 4326));
#     """
#     utc_time = int(time.time())
#     for cluster, points in hulls.items():
#         cluster_polygon_points = []
#         for point in points:
#             cluster_polygon_points.append([round(point[0], 6), round(point[1], 6)])
#
#         cluster_polygon_points.append([round(points[0][0], 6), round(points[0][1], 6)])
#         name = f"dbscan_cluster_{utc_time}_{cluster}"
#         polygon_wkt = f"POLYGON(({', '.join([f'{lon} {lat}' for lon, lat in cluster_polygon_points])}))"
#         type_id = 6
#         lng = cluster_polygon_points[0][0]
#         lat = cluster_polygon_points[0][1]
#         q = insert_query % (name, type_id, lng, lat, polygon_wkt)
#         cursor.execute(q)
#
#     conn.commit()
#
#     # Close the connection
#     cursor.close()
#     conn.close()

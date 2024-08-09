# -*- encoding: utf-8 -*-
'''
@File    :   hull_polygon.py
@Time    :   2024/07/25 20:21:04
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

# here put the import lib
import pandas as pd
import numpy as np

import pymssql
import time

from sklearn.cluster import DBSCAN
from scipy.spatial import ConvexHull, QhullError
from core.conf import sql_server_properties


# Function to compute convex hull and return the coordinates
def compute_convex_hull(df):
    unique_clusters = df['cluster'].unique()
    hulls = {}
    
    for cluster in unique_clusters:
        cluster_points = df[df['cluster'] == cluster][['lng', 'lat']].values
        if len(cluster_points) > 2:  # Convex hull requires at least 3 points
            try:
                hull = ConvexHull(cluster_points)
                hull_points = cluster_points[hull.vertices]
                hulls[cluster] = hull_points
            except QhullError as e:
                print(f"QhullError for cluster {cluster}: {e}")
        else:
            hulls[cluster] = cluster_points  # For less than 3 points, just use the points themselves
    
    return hulls


def hull_points_to_sql_server(df):
    hulls = compute_convex_hull(df)
    conn = pymssql.connect(sql_server_properties['host'], sql_server_properties['user'], 
                           'Amacs@0212', sql_server_properties['database'])
    cursor = conn.cursor()

    # Insert polygon data
    insert_query = """
    INSERT INTO ShoreNet.tab_dock_polygon (Name, type_id, lng, lat, Polygon)
    VALUES (N'%s', %d, %f, %f, geometry::STGeomFromText('%s', 4326));
    """
    utc_time = int(time.time())
    for cluster, points in hulls.items():
        cluster_polygon_points = []
        for point in points:
            cluster_polygon_points.append([round(point[0], 6), round(point[1], 6)])
            
        cluster_polygon_points.append([round(points[0][0], 6), round(points[0][1], 6)])
        name = f"dbscan_cluster_{utc_time}_{cluster}"
        polygon_wkt = f"POLYGON(({', '.join([f'{lon} {lat}' for lon, lat in cluster_polygon_points])}))"
        type_id = 6
        lng = cluster_polygon_points[0][0]
        lat = cluster_polygon_points[0][1]
        q = insert_query % (name, type_id, lng, lat, polygon_wkt)
        cursor.execute(q)
    
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()

"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  dock.py
@DateTime:  19/10/2024 12:52 am
"""

import re

import numpy as np
from pandas.core.frame import DataFrame
from sqlalchemy import func, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import engine
from sklearn.cluster import DBSCAN

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon


def get_dock_polygon(con: engine):
    """
    get dock polygon from sql server
    :return: [{'dock_id': ..., 'name': ..., 'polygon': [...], 'province': ... }]
    """
    Session = sessionmaker(bind=con)
    session = Session()
    polygons = session.query(
        DimDockPolygon.Id,
        DimDockPolygon.Name,
        func.ST_AsText(DimDockPolygon.Polygon).label('Polygon'),
        DimDockPolygon.lng,
        DimDockPolygon.lat
    ).filter(
        or_(DimDockPolygon.type_id == 1, DimDockPolygon.type_id == 6)
    ).filter(
        or_(DimDockPolygon.stage_id == 5, DimDockPolygon.stage_id is None)
    ).order_by(
        DimDockPolygon.Id
    ).all()

    dock_polygon_list = []
    for polygon in polygons:
        wkt_polygon = polygon.Polygon
        pattern = re.compile(r'\d+\.\d+\s\d+\.\d+')
        matches = pattern.findall(wkt_polygon)
        coordinates = [[float(coord) for coord in match.split()] for match in matches]
        dock_polygon_list.append(
            {
                'dock_id': polygon.Id, 'name': polygon.Name, 'polygon': coordinates, 'lng': polygon.lng, 'lat': polygon.lat
            }
        )
    print(f"Dock polygon count: {len(dock_polygon_list)}")
    session.close()
    return dock_polygon_list


def cluster_dock_polygon_dbscan(
        events_df: DataFrame,
        var: VariablesManager
):
    # dbscan all_coal_df
    coords = events_df[['lng', 'lat']].values

    kms_per_radian = 6371.0088
    epsilon = 0.2 / kms_per_radian

    # DBSCAN clustering
    db = DBSCAN(eps=epsilon, min_samples=30, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))

    # Add cluster labels to the DataFrame
    # events_df['cluster'] = db.labels_
    return db.labels_

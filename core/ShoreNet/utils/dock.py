"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  dock.py
@DateTime:  19/10/2024 12:52 am
"""

import re

from sqlalchemy import func, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import engine

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

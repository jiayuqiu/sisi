# -*- encoding: utf-8 -*-
'''
@File    :   DimDockPolygon.py
@Time    :   2024/08/09 20:24:52
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

from sisi_ops.utils.db.o2m.ShoreNet.base import Base

from sqlalchemy import Column, Integer, String, Float, Index
from geoalchemy2 import Geometry



# Define the Docks model
class DimDockPolygon(Base):
    __tablename__ = 'dim_dock_polygon'
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String(255), nullable=False)
    Polygon = Column(Geometry('POLYGON', srid=4326), nullable=False)
    Province = Column(String(255), nullable=True)
    Distruct = Column(String(255), nullable=True)
    lng = Column(Float, nullable=True)
    lat = Column(Float, nullable=True)
    type_id = Column(Integer, nullable=True)
    stage_id = Column(Integer, nullable=True)
    
    __table_args__ = (
        Index(
            'idx_dock_polygon',  # Index name
            'Id',
            'Province',
            'Distruct',
            'lng',
            'lat',
            'type_id'
        ),
    )

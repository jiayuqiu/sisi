# -*- encoding: utf-8 -*-
'''
@File    :   FactorAllStopEvent.py
@Time    :   2025/02/11 21:33:05
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''


from sisi_ops.utils.db.o2m.ShoreNet.base import Base

from sqlalchemy import Column, Integer, String, Float, BigInteger, Index


# Define the ORM class for the table
class FactorAllStopEvents(Base):
    __tablename__ = 'factor_all_stop_events'

    attribute_name_mapping = {
        "begin_year": "year",
        "begin_quarter": "quarter",
        "begin_month": "month",
        "begin_day": "day"
    }

    event_id = Column(String(255), primary_key=True, nullable=False)
    mmsi = Column(Integer, nullable=False)
    mmsi_cate = Column(String(255))
    ship_name = Column(String(255))
    imo = Column(String(255))
    begin_time = Column(BigInteger)
    end_time = Column(BigInteger)
    year = Column("begin_year", Integer)
    quarter = Column("begin_quarter", Integer)
    month = Column("begin_month", Integer)
    day = Column("begin_day", Integer)
    begin_lng = Column(Float)
    begin_lat = Column(Float)
    end_lng = Column(Float)
    end_lat = Column(Float)
    middle_lng = Column(Float)
    middle_lat = Column(Float)
    avg_lng = Column(Float)
    avg_lat = Column(Float)
    middle_hdg = Column(Float)
    max_hdg = Column(Float)
    min_hdg = Column(Float)
    middle_sog = Column(Float)
    max_sog = Column(Float)
    middle_cog = Column(Float)
    max_cog = Column(Float)
    min_cog = Column(Float)
    max_rot = Column(Float)
    min_rot = Column(Float)
    middle_rot = Column(Float)
    point_num = Column(Integer)
    avg_speed = Column(Float)
    avg_steady_speed = Column(Float)
    sailing_distance = Column(Float)
    zone_id = Column(Integer)
    navistate = Column(String(255))
    now_port_name = Column(String(255))
    now_port_id = Column(Integer)
    now_dock_name = Column(String(255))
    now_dock_id = Column(Integer)
    now_berth_name = Column(String(255))
    now_berth_id = Column(Integer)
    coal_dock_id = Column(Integer)
    now_port_lng = Column(Float)
    now_port_lat = Column(Float)
    event_categories = Column(String(255))
    province = Column(String(255))
    country = Column(String(255))
    event_cate = Column(String(255))

    
    # Define the index (optional, for reference)
    __table_args__ = (
        Index(
            'idx_common',  # Index name
            'mmsi', 
            'begin_year', 
            'begin_month', 
            'begin_day', 
            'begin_time', 
            'end_time', 
            'avg_speed', 
            'coal_dock_id', 
            'zone_id'
        ),
    )

    def __getattribute__(self, name):
        return Base.__getattribute__(self, name)
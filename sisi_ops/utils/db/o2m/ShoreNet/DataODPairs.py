from sisi_ops.utils.db.o2m.ShoreNet.base import Base
from sqlalchemy import Column, BigInteger, Float, Integer

class DataODPairs(Base):
    __tablename__ = 'data_od_pairs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    mmsi = Column(BigInteger, nullable=True)
    departure_dock_id = Column(BigInteger, nullable=True)
    departure_time = Column(BigInteger, nullable=True)
    departure_year = Column(BigInteger, nullable=True)
    departure_month = Column(BigInteger, nullable=True)
    departure_quarter = Column(BigInteger, nullable=True)
    departure_lng = Column(Float, nullable=True)
    departure_lat = Column(Float, nullable=True)
    arrival_dock_id = Column(BigInteger, nullable=True)
    arrival_time = Column(Float, nullable=True)
    arrival_year = Column(BigInteger, nullable=True)
    arrival_month = Column(BigInteger, nullable=True)
    arrival_quarter = Column(BigInteger, nullable=True)
    arrival_lng = Column(Float, nullable=True)
    arrival_lat = Column(Float, nullable=True)
    sail_duration = Column(BigInteger, nullable=True)

    # __table_args__ = (
    #     Index('idx_mmsi_dock_time', 'mmsi', 'departure_dock_id', 'arrival_dock_id', 'departure_time', 'arrival_time'),
    # )

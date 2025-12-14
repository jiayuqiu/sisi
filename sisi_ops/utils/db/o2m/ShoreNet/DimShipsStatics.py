from sisi_ops.utils.db.o2m.ShoreNet.base import Base
from sqlalchemy import Column, BigInteger, Integer, Text, Float, Index, String

class DimShipsStatics(Base):
    __tablename__ = 'dim_ships_statics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date_id = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    receivetime = Column(BigInteger, nullable=False)
    mmsi = Column(BigInteger, nullable=False)
    # msg_time = Column(BigInteger, nullable=False, comment="message time from ais")
    ship_name = Column(Text(collation="utf8mb4_bin"), nullable=False)
    ship_type = Column(Float, nullable=False)
    imo = Column(String(100), nullable=True)
    callsign = Column(String(100), nullable=True)
    length = Column(Float, nullable=False)
    width = Column(Float, nullable=False)
    length_width_ratio = Column(Float, nullable=False)
    dwt = Column(Float, nullable=True)


    __table_args__ = (
        Index(
            "idx_ships_statics",
            "date_id",
            "year",
            "month",
            "mmsi",
            "ship_type"
        ),
    )

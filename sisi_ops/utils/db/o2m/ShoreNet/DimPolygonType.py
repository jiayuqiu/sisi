from sisi_ops.utils.db.o2m.ShoreNet.base import Base
from sqlalchemy import Column, Integer, String, Index

class DimPolygonType(Base):
    __tablename__ = 'dim_polygon_type'

    id = Column(Integer, primary_key=True, autoincrement=True)
    type_id = Column(Integer, nullable=False)
    polygon_type_desc_eng = Column(String(255, collation="utf8mb4_bin"), nullable=True)
    polygon_type_sec_chn = Column(String(255, collation="utf8mb4_bin"), nullable=True)

    __table_args__ = (
        Index('idx_type_id', 'type_id'),  # Index for `type_id`
    )

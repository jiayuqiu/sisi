from sqlalchemy import create_engine, MetaData, Column, Integer, String
from sqlalchemy.orm import declarative_base, Session
from geoalchemy2 import Geometry, Geography
from geoalchemy2.shape import from_shape
from shapely.geometry import Polygon

from core.conf import ss_engine as engine

# # Database connection details
# username = 'sa'
# password = 'Amacs%400212'
# host = '172.18.128.1'
# database = 'SISI'
#
# # Create an engine and connect to the database
# engine = create_engine(f'mssql+pymssql://{username}:{password}@{host}:21433/{database}')

# Define metadata
Base = declarative_base()

# Define the table structure
class DockData(Base):
    __tablename__ = 'dock_data'
    __table_args__ = {'schema': 'ShoreNet'}  # Adjust the schema name if necessary
    dock_id = Column(Integer, primary_key=True)  # No autoincrement
    dock_name = Column(String(50))
    dock_polygon = Column(Geometry('POLYGON'))

# Create the table (if it doesn't exist)
# Base.metadata.create_all(engine)

# Create a new session
with Session(engine) as session:
    try:
        # Create a polygon using Shapely
        polygon = Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)])

        # Insert data into the table
        new_dock = DockData(
            dock_id=1,  # Manually specify the dock_id value
            dock_name='Dock A',
            dock_polygon=from_shape(polygon, srid=4326)  # SRID is the Spatial Reference System Identifier
        )

        # Add and commit the new dock data
        session.add(new_dock)
        session.commit()

        print("Data inserted successfully.")

    except Exception as e:
        # Rollback the session in case of any error
        session.rollback()
        print(f"An error occurred: {e}")

    finally:
        # Close the session
        session.close()

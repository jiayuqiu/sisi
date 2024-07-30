import pandas as pd
import pymssql

import os, sys
import platform
os_name = platform.system()
if os.name == 'nt' or os_name == 'Windows':
    DATA_PATH = r"D:/data/sisi/"
elif os.name == 'posix' or os_name == 'Linux':
    DATA_PATH = r"/mnt/d/data/sisi/"
else:
    DATA_PATH = "/mnt/d/data/sisi/"

parent_path = os.path.abspath('.')
sys.path.append(parent_path)
print(parent_path)
parent_path = os.path.abspath('..')
sys.path.append(parent_path)
print(parent_path)
parent_path = os.path.abspath('../../')
sys.path.append(parent_path)
print(parent_path)
parent_path = os.path.abspath('../../../')
print(parent_path)
sys.path.append(parent_path)
print(sys.path)

from core.ShoreNet.scripts.parse_kml import parse_kml_document
    

# Define the connection details
server = '127.0.0.1'
user = 'sa'
password = 'Amacs@0212'
database = 'sisi'

# Connect to SQL Server
conn = pymssql.connect(server, user, password, database)
cursor = conn.cursor()

file_name = os.path.join(DATA_PATH, 'dock/extensive_20240730_v2.kml')
dock_polygon_list = parse_kml_document(file_name)
print(f"dock polygon count: {len(dock_polygon_list)}")
print(dock_polygon_list[:5])


# Insert polygon data
insert_query = """
INSERT INTO ShoreNet.tab_dock_polygon (Name, Polygon)
VALUES (N'%s', geometry::STGeomFromText('%s', 4326));
"""

for row in dock_polygon_list:
    name = row['name']
    # province = row['province']
    polygon_coords = row['polygon']
    polygon_wkt = f"POLYGON(({', '.join([f'{lon} {lat}' for lon, lat in polygon_coords])}))"
    q = insert_query % (name, polygon_wkt)
    cursor.execute(q)

conn.commit()

# Close the connection
cursor.close()
conn.close()

from sqlalchemy.orm import sessionmaker
from geoalchemy2 import WKTElement

import json
import codecs
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

# from core.ShoreNet.scripts.parse_kml import parse_kml_document
from sisi_ops.conf import mysql_engine
from sisi_ops.ShoreNet.utils.amap import amap_request
from sisi_ops.utils.db.o2m.ShoreNet import DimDockPolygon


def insert_data():
    Session = sessionmaker(bind=mysql_engine)
    session = Session()

    # # load kml file
    # file_name = os.path.join(DATA_PATH, 'dock/extensive_20240730_v2.kml')
    # dock_polygon_list = parse_kml_document(file_name)
    # print(f"dock polygon count: {len(dock_polygon_list)}")
    # print(dock_polygon_list[:5])

    # # load json file
    file_name = os.path.join(DATA_PATH, 'ShoreNet/dock_polygon.json')
    with codecs.open(file_name, 'r', 'utf-8-sig') as f:
        dock_data = json.load(f)

    for dock in dock_data:
        polygon_coords = dock['polygon']
        polygon_wkt = f"POLYGON(({', '.join([f'{coord[1]} {coord[0]}' for coord in polygon_coords])}))"

        # Create a DimDockPolygon instance
        new_dock = DimDockPolygon(
            Name=dock['name'],
            Polygon=WKTElement(polygon_wkt, srid=4326),  # srid=4326 is commonly used for GPS coordinates
            Province=dock['province'],
            Distruct=None,  # Assuming there is no 'Distruct' key in the JSON, set to None
            lng=dock['lng'],
            lat=dock['lat'],
            type_id=None,  # Assuming 'type_id' is not present in the JSON, set to None
            stage_id=None  # Assuming 'stage_id' is not present in the JSON, set to None
        )

        # Add and commit the new dock entry
        session.add(new_dock)

    # Commit all the changes
    session.commit()

    # Close the session
    session.close()


def update_location():
    Session = sessionmaker(bind=mysql_engine)
    session = Session()
    
    data_to_update = session.query(
        DimDockPolygon
    ).filter(
        DimDockPolygon.Distruct == None
    ).all()
    
    n = len(data_to_update)
    execute_count = 0
    for row in data_to_update:
        print(f"{execute_count} / {n}")
        localtion_dict = amap_request(row.lng, row.lat)
        province = localtion_dict['regeocode']['addressComponent']['province']
        district = localtion_dict['regeocode']['addressComponent']['district']
        
        if isinstance(district, str):
            session.query(
                DimDockPolygon
            ).filter(
                DimDockPolygon.Id == row.Id
            ).update(
                {
                    DimDockPolygon.Province: province,
                    DimDockPolygon.Distruct: district
                }
            )
            
        execute_count += 1
        if execute_count % 50 == 0:
            session.commit()
    
    # Commit all changes
    session.commit()

    # Close the session
    session.close()


if __name__ == '__main__':
    update_location()
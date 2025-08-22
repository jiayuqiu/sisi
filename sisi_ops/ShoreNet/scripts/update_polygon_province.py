import pandas as pd
import numpy as np
import pymssql
import requests

from pprint import pprint
import os
import sys
parent_path = os.path.abspath('..')
sys.path.append(parent_path)
parent_path = os.path.abspath('../../')
sys.path.append(parent_path)
parent_path = os.path.abspath('../../../')
sys.path.append(parent_path)
print(sys.path)


def amap_request(lng, lat):
    # https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=<用户的key>&radius=1000&extensions=all
    key = "083c8dacc6cadcd2bd390f59c8057547"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}
    request_url = (f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={lng},{lat}&key={key}&"
                   f"radius=1000&extensions=base")
    response = requests.get(request_url, headers=headers)
    # print(j['regeocode']['addressComponent']['province'], j['regeocode']['addressComponent']['district'])
    return response.json()


def load_polygon():
    # load data from sql server
    # load dock polygon
    # Define the connection details
    server = '127.0.0.1'
    user = 'sa'
    password = 'Amacs@0212'
    database = 'sisi'

    # Connect to SQL Server
    conn = pymssql.connect(server, user, password, database)
    cursor = conn.cursor()

    query = "SELECT Id, Name, Polygon.STAsText() as Polygon FROM ShoreNet.dim_dock_polygon t " + \
            "WHERE t.Distruct is NULL;"
    dock_df = pd.read_sql(query, conn)

    import re
    dock_polygon_list = []
    
    execute_count = 0
    for _, row in dock_df.iterrows():
        print(f"{_} / {dock_df.shape[0]}")
        wkt_polygon = row['Polygon']
        pattern = re.compile(r'\d+\.\d+\s\d+\.\d+')
        matches = pattern.findall(wkt_polygon)
        coordinates = [[float(coord) for coord in match.split()] for match in matches]
        localtion_dict = amap_request(coordinates[0][0], coordinates[0][1])
        province = localtion_dict['regeocode']['addressComponent']['province']
        district = localtion_dict['regeocode']['addressComponent']['district']
        print(province, district)
        if district == '[]':
            cursor.execute(f"UPDATE ShoreNet.dim_dock_polygon "
                           f"SET Province = N'{province}' "
                           f", Distruct = NULL "
                           f", lng = {coordinates[0][0]} , lat = {coordinates[0][1]} WHERE Id = {row['Id']}")
        else:
            cursor.execute(f"UPDATE ShoreNet.dim_dock_polygon "
                           f"SET Province = N'{province}' "
                           f", Distruct = N'{district}' "
                           f", lng = {coordinates[0][0]} , lat = {coordinates[0][1]} WHERE Id = {row['Id']}")
        
        execute_count += 1
        if execute_count % 50 == 0:
            conn.commit()

    # Close the connection
    conn.commit()
    conn.close()
    return None


if __name__ == '__main__':
    load_polygon()

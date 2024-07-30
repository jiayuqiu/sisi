import re
import os
import sys
import json
import platform

import pymssql
import pandas as pd
import numpy as np
from sqlalchemy import text

import dask

# Set Dask to use cuDF as the backend for dataframe operations
dask.config.set({"dataframe.backend": "cudf"})

import dask.dataframe as dd

parent_path = os.path.abspath('.')
sys.path.append(parent_path)
parent_path = os.path.abspath('../')
sys.path.append(parent_path)
parent_path = os.path.abspath('../../')
sys.path.append(parent_path)
print(sys.path)

from core.conf import ss_engine
from core.ShoreNet.utils.geo import point_poly, get_geodist
from core.conf import sql_server_properties
from core.ShoreNet.scripts.sail_log_match_polygon import find_dock, get_dock_polygon

os_name = platform.system()
if os.name == 'nt' or os_name == 'Windows':
    DATA_PATH = r"D:/data/sisi/"
elif os.name == 'posix' or os_name == 'Linux':
    DATA_PATH = r"/mnt/d/data/sisi/"
else:
    DATA_PATH = r"/mnt/d/data/sisi/"

STAGE_ID = 3


def merge_files():
    use_fields = [
                    'mmsi', 'Begin_time', 'End_time', 'Begin_lon', 'Begin_lat',
                    'avgSpeed', 'Point_num', 'Event_categories'
                ]

    start_month, end_month = 1, 12
    months = [f"2023{x:02}" for x in range(start_month, end_month+1)]
    month_sail_df_list = []
    for month in months:
        print(month)
        print(os.path.join(DATA_PATH, f"log_data/{month}_new_sailingv4.csv"))
        month_sail_df = pd.read_csv(os.path.join(DATA_PATH, f"log_data/{month}_new_sailingv4.csv"),
                                    skipinitialspace=True, usecols=use_fields)
        month_sail_df_list.append(month_sail_df)
        
    sail_df = pd.concat(month_sail_df_list, ignore_index=True)
    try:
        sail_df.to_parquet(os.path.join(DATA_PATH, f"log_data/2023_sail_events_usecols.parquet"), index=False)
    except:
        print("output failed!")
        

def find_dock_cuda(event_row, dock_list):
    """
    calculate the distance between the first point of the event and the first point of the dock polygon.
    """
    # dock_list = get_dock_list()
    from core.ShoreNet.utils.geo import point_poly, get_geodist
    # if np.random.randint(0, 100, 1)[0] < 5:
    #     print(f"{month}, {event_row['mmsi']}")
    
    geodict_list = []
    for polygon in dock_list:
        for d_lng, d_lat in polygon['polygon']:
            geodist = get_geodist(event_row['lng'], event_row['lat'], d_lng, d_lat)
            geodict_list.append(geodist)
        
        min_geodict = min(geodict_list)
        if min_geodict < 8:
            # geodist less than 5 km, check the point whether is in the polygon
            if point_poly(event_row['lng'], event_row['lat'], polygon['polygon']):
                return polygon['dock_id']
    return -1


def ff(row, l):
    return f"{row['mmsi']} + l"


if __name__ == '__main__':
    # dock_list = get_dock_polygon()
    # docks_dict = {'docks': dock_list}
    # print(docks_dict)
    # docks_string = json.dumps(docks_dict)
    # print(docks_string)
    
    df = dd.read_parquet(os.path.join(DATA_PATH, "log_data/2023_sail_events_usecols.parquet"))
    t_df = df.loc[df['mmsi'] > 1000000].head()
    # print(t_df.head())
    print(t_df.apply(ff, axis=1, args=('abc', )))

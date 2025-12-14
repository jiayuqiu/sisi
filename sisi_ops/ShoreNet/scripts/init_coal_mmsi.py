# -*- encoding: utf-8 -*-
'''
@File    :   init_coal_mmsi.py
@Time    :   2024/07/27 20:16:27
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

# here put the import lib
import pandas as pd

import sys
import os
import re
import json
import platform

parent_path = os.path.abspath('.')
sys.path.append(parent_path)
parent_path = os.path.abspath('../')
sys.path.append(parent_path)
parent_path = os.path.abspath('../../')
sys.path.append(parent_path)
print(sys.path)

os_name = platform.system()
if os.name == 'nt' or os_name == 'Windows':
    DATA_PATH = r"D:/data/sisi/"
elif os.name == 'posix' or os_name == 'Linux':
    DATA_PATH = r"/mnt/d/data/sisi/"
else:
    DATA_PATH = r"/mnt/d/data/sisi/"

from sisi_ops.conf import ss_engine
from sisi_ops.ShoreNet.scripts.sail_log_match_polygon import find_dock



def get_coal_dock_polygon():
    """
    get dock polygon from sql server
    :return: [{'dock_id': ..., 'name': ..., 'polygon': [...], 'province': ... }]
    """
    query = "SELECT Id, Name, Polygon.STAsText() as Polygon, Province FROM sisi.ShoreNet.tab_dock_polygon WHERE type_id = 1 ORDER BY Id;"
    with ss_engine.connect() as con:
        dock_df = pd.read_sql(query, con)

    dock_polygon_list = []
    for _, row in dock_df.iterrows():
        wkt_polygon = row['Polygon']
        pattern = re.compile(r'\d+\.\d+\s\d+\.\d+')
        matches = pattern.findall(wkt_polygon)
        coordinates = [[float(coord) for coord in match.split()] for match in matches]
        # if row['Id'] in [9, 10]:
        dock_polygon_list.append(
            {
                'dock_id': row['Id'], 'name': row['Name'], 'polygon': coordinates, 'province': row['Province']
            }
        )
    print(f"Dock polygon count: {len(dock_polygon_list)}")
    return dock_polygon_list


class InitCoalList(object):
    lng_field_name = 'Begin_lon'
    lat_field_name = 'Begin_lat'

    def __init__(self, start_month, end_month):
        # sailing log use fields
        self.use_fields = [
            'mmsi', 'Begin_time', 'End_time', self.lng_field_name, self.lat_field_name, 'avgSpeed'
        ]

        self.months = [f"2023{x:02}" for x in range(start_month, end_month+1)]

        self.docks = get_coal_dock_polygon()
    
    def load_sail_log(self, month):
        sail_df = pd.read_csv(f"{DATA_PATH}/log_data/{month}_new_sailingv4.csv",
                              skipinitialspace=True, usecols=self.use_fields)

        sail_df.rename(columns={f'{self.lng_field_name}': 'lng', f'{self.lat_field_name}': 'lat'}, inplace=True)
        sail_df.loc[:, 'lng'] = sail_df['lng'].multiply(0.000001)
        sail_df.loc[:, 'lat'] = sail_df['lat'].multiply(0.000001)
        print(sail_df.describe())

        # select china region
        sail_df = sail_df.loc[(sail_df['lng'] > 105.5) & (sail_df['lng'] < 126) &
                              (sail_df['lat'] > 18) & (sail_df['lat'] < 41.6) &
                              (sail_df['avgSpeed'] < 1)]
        
        return sail_df

    def run(self, ):
        from pandarallel import pandarallel
        pandarallel.initialize(progress_bar=False, nb_workers=12)
        
        for month in self.months:
            print(f"{month} start find polygon...")
            try:
                month_sail_df = self.load_sail_log(month)
            except:
                print(f"{month} load failed.!!!!!!!!!!!!!!!!!!!!!")
                continue
                
            dock_tag = month_sail_df.parallel_apply(find_dock, args=(self.docks, ), axis=1)
            month_coal_event_df = month_sail_df.loc[:, 'coal_dock_id'] = dock_tag
            
            month_coal_event_df = month_coal_event_df.loc[~month_coal_event_df['coal_dock_id'].isna()]
            month_coal_event_df.to_csv(os.path.join(DATA_PATH, f"init_coal_mmsi/{month}_coal_dock_event.csv"), index=False)
            print(month_coal_event_df.shape)
    
    def init_mmsi(self, ):
        all_coal_sail_df_list = []
        for month in self.months:
            coal_sail_df = pd.read_csv(os.path.join(DATA_PATH, f"init_coal_mmsi/{month}_coal_dock_event.csv"))
            all_coal_sail_df_list.append(coal_sail_df)

        all_coal_sail_df = pd.concat(all_coal_sail_df_list, ignore_index=True)
        coal_mmsi_list = list(set([int(m) for m in all_coal_sail_df['mmsi'].unique()]))
        with open('/home/qiu/ideaProjects/SISI/core/ShoreNet/scripts/coal_mmsi_v1.json', 'w') as f:
            json.dump({'mmsi': coal_mmsi_list}, f)


def main():
    icl = InitCoalList(1, 12)
    icl.init_mmsi()


if __name__ == '__main__':
    main()
import re
import os
import sys
import json
import platform
import traceback

import pymssql
import pandas as pd
import numpy as np
from sqlalchemy import text, and_, or_, func
from sqlalchemy.orm import sessionmaker

from shapely.wkt import loads as load_wkt

parent_path = os.path.abspath('.')
sys.path.append(parent_path)
parent_path = os.path.abspath('../')
sys.path.append(parent_path)
parent_path = os.path.abspath('../../')
sys.path.append(parent_path)
print(sys.path)

from core.conf import mysql_engine
from core.ShoreNet.utils.geo import point_poly, get_geodist
from core.conf import sql_server_properties
from core.ShoreNet.utils.get_stage_id import get_stage_id
from core.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon

os_name = platform.system()
if os.name == 'nt' or os_name == 'Windows':
    DATA_PATH = r"D:/data/sisi/"
elif os.name == 'posix' or os_name == 'Linux':
    DATA_PATH = r"/mnt/d/data/sisi/"
else:
    DATA_PATH = r"/mnt/d/data/sisi/"


STAGE_ID = get_stage_id()
STAGE_ID += 1


def truncate_sailing():
    with mysql_engine.connect() as con:
        con.execute(text("TRUNCATE TABLE factor_stop_events"))


def find_dock(event_row, dock_list):
    """
    calculate the distance between the first point of the event and the first point of the dock polygon.
    """
    from core.ShoreNet.utils.geo import point_poly, get_geodist

    for polygon in dock_list:
        dst_list = []
        for d_lng, d_lat in polygon['polygon']:
            geodist = get_geodist(event_row['lng'], event_row['lat'], d_lng, d_lat)
            dst_list.append(geodist)
            
        if min(dst_list) < 15:
            if point_poly(event_row['lng'], event_row['lat'], polygon['polygon']):
                return polygon['dock_id']


def get_dock_polygon():
    """
    get dock polygon from sql server
    :return: [{'dock_id': ..., 'name': ..., 'polygon': [...], 'province': ... }]
    """
    Session = sessionmaker(bind=mysql_engine)
    session = Session()
    polygons = session.query(
        DimDockPolygon.Id,
        DimDockPolygon.Name,
        func.ST_AsText(DimDockPolygon.Polygon).label('Polygon')
    ).filter(
        or_(DimDockPolygon.type_id == 1, DimDockPolygon.type_id == 6)
    ).filter(
        or_(DimDockPolygon.stage_id == 5, DimDockPolygon.stage_id == None)
    ).order_by(
        DimDockPolygon.Id
    ).all()

    dock_polygon_list = []
    for polygon in polygons:
        wkt_polygon = polygon.Polygon
        pattern = re.compile(r'\d+\.\d+\s\d+\.\d+')
        matches = pattern.findall(wkt_polygon)
        coordinates = [[float(coord) for coord in match.split()] for match in matches]
        dock_polygon_list.append(
            {
                'dock_id': polygon.Id, 'name': polygon.Name, 'polygon': coordinates
            }
        )
    print(f"Dock polygon count: {len(dock_polygon_list)}")
    session.close()
    return dock_polygon_list


class DockDBSCAN(object):
    lng_field_name = 'Begin_lon'
    lat_field_name = 'Begin_lat'

    def __init__(self, start_month, end_month):
        # sailing log use fields
        self.use_fields = [
            'mmsi', 'Begin_time', 'End_time', self.lng_field_name, self.lat_field_name, 'avgSpeed', 'Point_num', 
            'Event_categories'
        ]

        self.months = [f"2023{x:02}" for x in range(start_month, end_month+1)]

        self.docks = get_dock_polygon()

    def load_sail_log(self, month):
        sail_df = pd.read_csv(f"{DATA_PATH}/log_data/{month}_new_sailingv4.csv",
                              skipinitialspace=True, usecols=self.use_fields)
            
        sail_df.rename(columns={f'{self.lng_field_name}': 'lng', f'{self.lat_field_name}': 'lat'}, inplace=True)
        sail_df.loc[:, 'lng'] = sail_df['lng'].multiply(0.000001)
        sail_df.loc[:, 'lat'] = sail_df['lat'].multiply(0.000001)

        sail_df = sail_df.loc[(sail_df['lng'] > 105.5) & (sail_df['lng'] < 126) &
                              (sail_df['lat'] > 18) & (sail_df['lat'] < 41.6) &
                              (sail_df['avgSpeed'] < 1)]
        # sail_df = sail_df.iloc[:10000, :]
        sail_df.drop_duplicates(subset=['mmsi', 'Begin_time', 'End_time'], keep='first')
        return sail_df

    @staticmethod
    def get_coal_mmsi_list_init():
        coal_mmsi_file_name = '/mnt/d/IdeaProjects/SISI/core/ShoreNet/scripts/coal_mmsi_v1_init.json'
        rf = open(coal_mmsi_file_name, 'r')
        exists_mmsi_dict = json.load(rf)
        # print(f"exists mmsi count: {len(exists_mmsi_dict['mmsi'])}")
        rf.close()
        return exists_mmsi_dict

    @staticmethod
    def update_coal_mmsi_list(sail_df):
        # get coal mmsi list
        coal_mmsi_list = sail_df.loc[~sail_df['coal_dock_id'].isna()]['mmsi'].values.tolist()

        # save to json
        coal_mmsi_dict = {'mmsi': coal_mmsi_list}

        coal_mmsi_file_name = '/home/qiu/ideaProjects/SISI/core/ShoreNet/scripts/coal_mmsi_v1.json'
        if os.path.exists(coal_mmsi_file_name):
            print(f"exists coal_mmsi, append mmsi")
            rf = open(coal_mmsi_file_name, 'r')
            exists_mmsi_dict = json.load(rf)
            print(f"exists mmsi count: {len(exists_mmsi_dict['mmsi'])}")
            rf.close()

            coal_mmsi_dict['mmsi'].extend(exists_mmsi_dict['mmsi'])
            coal_mmsi_dict['mmsi'] = list(set(coal_mmsi_dict['mmsi']))
            print(f"updated mmsi count: {len(coal_mmsi_dict['mmsi'])}")
            wf = open(coal_mmsi_file_name, 'w')
            json.dump(coal_mmsi_dict, wf)
            wf.close()
        else:
            with open(coal_mmsi_file_name, 'w') as f:
                json.dump(coal_mmsi_dict, f)
        return coal_mmsi_dict

    @staticmethod
    def save_sail_log(df):
        with mysql_engine.connect() as con:
            df.to_sql(
                name='factor_stop_events',
                schema='sisi',
                if_exists='append',
                index=False,
                con=con
            )
    
    @staticmethod
    def update_coal_dock_id(df):
        conn = pymssql.connect(sql_server_properties['host'], sql_server_properties['user'], 
                               'Amacs@0212', sql_server_properties['database'])
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            if not np.isnan(row['coal_dock_id']):
                cursor.execute(f"""
                            update 
                                sisi.tab_sailing_log
                            SET 
                                coal_dock_id = {row['coal_dock_id']}
                            WHERE
                                mmsi = {row['mmsi']} and Begin_time = {row['Begin_time']}
                            """)
        conn.commit()

        # Close the connection
        cursor.close()
        conn.close()
                
    def update(self, ):
        from pandarallel import pandarallel
        pandarallel.initialize(progress_bar=True, nb_workers=10)
        
        load_query = f"""
        select 
            -- top 10000
            mmsi, Begin_time, End_time, lng, lat, avgSpeed
        from 
            sisi.ShoreNet.tab_sailing_log
        where 
            coal_dock_id is NULL and avgSpeed < 1
        """
        events_df = pd.read_sql(
            sql=load_query,
            con=mysql_engine
        )
        
        # update_dock_id_list = [6023, 6022, 6021, 6020, 6019, 6018, 6017, 6016]
        update_polygon_list = [d for d in self.docks if d['dock_id'] >= 6024]
        
        dock_tag = events_df.parallel_apply(find_dock, args=(update_polygon_list, ), axis=1)
        events_df.loc[:, 'coal_dock_id'] = dock_tag
        # events_df.loc[:, 'stage_id'] = [STAGE_ID] * events_df.shape[0]
        self.update_coal_dock_id(events_df.loc[~events_df['coal_dock_id'].isna()])
        
    def run(self, if_truncate=True):
        if if_truncate:
            truncate_sailing()

        # sys.exit(1)
        from pandarallel import pandarallel
        pandarallel.initialize(progress_bar=True, nb_workers=10)

        coal_sail_df_list = []
        for month in self.months:
            print(f"{month} start find polygon...")
            try:
                month_sail_df = self.load_sail_log(month)
            except:
                print(f"{month} load failed.!!!!!!!!!!!!!!!!!!!!!")
                continue

            dock_tag = month_sail_df.parallel_apply(find_dock, args=(self.docks, ), axis=1)
            month_sail_df.loc[:, 'coal_dock_id'] = dock_tag
            month_sail_df.loc[:, 'stage_id'] = [STAGE_ID] * month_sail_df.shape[0]
            coal_sail_df_list.append(month_sail_df)

            # # update coal mmsi list
            # coal_mmsi_dict = self.update_coal_mmsi_list(month_sail_df)
            coal_mmsi_dict = self.get_coal_mmsi_list_init()
            
            coal_mmsi_list = [int(mmsi) for mmsi in coal_mmsi_dict['mmsi']]
            month_coal_sail_df = month_sail_df.loc[month_sail_df['mmsi'].isin(coal_mmsi_list)]

            # output monthly extensive sail log
            extensive_coal_dock_df = month_sail_df.loc[(month_sail_df['coal_dock_id'].isna()) &
                                                       (month_sail_df['mmsi'].isin(coal_mmsi_dict['mmsi']))]
            
            if not os.path.exists(os.path.join(f"{DATA_PATH}/extensive_coal_events/stage_{STAGE_ID}/")):
                os.makedirs(os.path.join(f"{DATA_PATH}/extensive_coal_events/stage_{STAGE_ID}/"))
                
            extensive_coal_dock_df.to_csv(f"{DATA_PATH}/extensive_coal_events/stage_{STAGE_ID}/{month}.csv",
                                          index=False, encoding='utf-8-sig')
            try:
                self.save_sail_log(month_coal_sail_df)
            except:
                print(f"Save failed. {month}")
                
            # print(month_coal_sail_df.loc[month_coal_sail_df['coal_dock_id'].isin([9, 10])].shape)
            print(f"{month} done!::::::::::::::::::::::::::::::::::::::::")

        # # merge coal sail logs
        coal_sail_df = pd.concat(coal_sail_df_list, ignore_index=True)
        coal_sail_df.sort_values(by=['mmsi', 'Begin_time'], inplace=True)
        coal_sail_df.to_csv(f"{DATA_PATH}/extensive_coal_events/stage_{STAGE_ID}/2023_events_with_polygon_{STAGE_ID}.csv",
                            index=False, encoding='utf-8-sig')


if __name__ == '__main__':
    dd = DockDBSCAN(start_month=1, end_month=12)
    print(dd.months)
    dd.run()
    # dd.update()
    # get_dock_polygon()

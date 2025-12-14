import pandas as pd
import numpy as np
from sqlalchemy import text
import traceback

import math

from sisi_ops.conf import ss_engine


def str_to_int(value):
    try:
        return int(value)
    except:
        return math.nan


df = pd.read_csv("/mnt/c/Users/qiu/OneDrive/文档/SISI/3沿海干散货研究/data/coal_mmsi_static.csv",
                 usecols=['mmsi', 'shipname', 'shiptype', 'length', 'breadth'])
cleaned_row_list = []
for mmsi, group in df.groupby('mmsi'):
    mmsi_static_dict = {'mmsi': mmsi}
    contains_ship_name_info = False
    contains_length_info = False
    contains_width_info = False
    contains_ship_type_info = False
    for _, g_row in group.iterrows():
        ship_name = g_row['shipname']
        length = str_to_int(g_row['length'])
        width = str_to_int(g_row['breadth'])
        ship_type = str_to_int(g_row['shiptype'])

        # add ship name
        if isinstance(ship_name, float):
            pass
        elif isinstance(ship_name, str):
            ship_name = ship_name.strip()
            if ship_name == '""':
                pass
            else:
                mmsi_static_dict['ship_name'] = ship_name

        # add length
        if ~np.isnan(length):
            mmsi_static_dict['length'] = length

        # add width
        if ~np.isnan(width):
            mmsi_static_dict['breadth'] = width

        # add ship_type
        if ~np.isnan(ship_type):
            mmsi_static_dict['ship_type'] = int(ship_type)

        # if all features captured, end
        if contains_ship_name_info & contains_length_info & contains_width_info & contains_ship_type_info:
            cleaned_row_list.append(mmsi_static_dict)
            break

    if contains_ship_name_info & contains_length_info & contains_width_info & contains_ship_type_info:
        continue
    else:
        cleaned_row_list.append(mmsi_static_dict)


print(cleaned_row_list[:5], len(cleaned_row_list), len(df.groupby('mmsi')))
static_df = pd.DataFrame(cleaned_row_list)
static_df.rename(columns={'breadth': 'width'}, inplace=True)

with ss_engine.connect() as con:
    try:
        con.execute(text("TRUNCATE TABLE sisi.ShoreNet.dim_ships_static"))
        print("Truncate ShoreNet.dim_ships_static successfully.")
    except:
        traceback.print_exc()
        print("Truncate ShoreNet.dim_ships_static failed.")

static_df.to_sql(
    name='dim_ships_static',
    schema='ShoreNet',
    if_exists='append',
    index=False,
    con=ss_engine
)

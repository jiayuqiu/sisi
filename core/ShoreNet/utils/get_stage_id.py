# -*- encoding: utf-8 -*-
'''
@File    :   get_stage_id.py
@Time    :   2024/08/09 15:01:43
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   get latest stage id from database
'''

import pandas as pd

import os, sys

parent_path = os.path.abspath('.')
sys.path.append(parent_path)
parent_path = os.path.abspath('../')
sys.path.append(parent_path)
parent_path = os.path.abspath('../../')
sys.path.append(parent_path)
print(sys.path)

from core.ShoreNet.conf import mysql_engine


def get_stage_id():
    query = """
    SELECT 
        stage_id
    FROM 
        sisi.dim_stage_version
    WHERE Id = (SELECT MAX(Id) FROM sisi.dim_stage_version)
    """
    with mysql_engine.connect() as con:
        df = pd.read_sql(
            sql=query,
            con=con
        )
    stage_id = int(df['stage_id'].values[0])
    return stage_id


if __name__ == '__main__':
    get_stage_id()

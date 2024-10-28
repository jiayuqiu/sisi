"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  tools.py
@DateTime:  27/10/2024 4:10 pm
@DESC    :  tools for statics
"""

import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy import text


def load_all_ship_statics(con: Engine) -> pd.DataFrame:
    query = """
    SELECT
        mmsi, ship_name, ship_type, length, width, dwt
    FROM
        sisi.dim_ships_statics
    """
    return pd.DataFrame()


def load_coal_ship_statics(con: Engine) -> pd.DataFrame:
    query = """
    SELECT
        mmsi, length, width, dwt
    FROM
        sisi.dim_ships_statics
    WHERE
        dwt > 0
    """
    df = pd.read_sql(
        sql=text(query), con=con
    )
    return df

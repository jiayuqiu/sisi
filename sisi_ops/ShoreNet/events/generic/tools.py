"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  tools.py
@DateTime:  27/10/2024 4:47 pm
@DESC    :  events tools
"""

import re

import pandas as pd

from sqlalchemy import func, text
from sqlalchemy.orm import sessionmaker

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager
from sisi_ops.infrastructure.definition.parameters import Prefix
from sisi_ops.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon
from sisi_ops.infrastructure.definition.parameters import WarehouseDefinitions as tbn


def load_events_month(year: int, month: int, vars: ShoreNetVariablesManager) -> pd.DataFrame:
    query = f"""
    SELECT
        event_id,
        mmsi,
        begin_time,
        end_time,
        begin_lng,
        begin_lat,
        avg_speed,
        event_categories,
        begin_year as year,
        begin_month as month
    FROM
        {vars.warehouse_schema}.{tbn.all_stop_events}
    WHERE
        begin_year = {year} AND begin_month = {month}
    """
    df = pd.read_sql(
        sql=query, 
        con=vars.engine
    )
    return df


def load_events_with_dock(year_list: list[str], vars: ShoreNetVariablesManager) -> pd.DataFrame:
    """
    load events with dock

    Args:
        year_list: list[str], a year list
        vars: ShoreNetVariablesManager, framework variables

    Returns:
        Pandas DataFrame
    """
    if len(year_list) >= 2:
        year_filter_str = ", ".join(year_list)
    else:
        year_filter_str = year_list[0]

    _df = pd.read_sql(
        sql=f"""
            SELECT
                event_id,
                mmsi,
                begin_time,
                end_time,
                end_time - begin_time as duration,
                begin_lng,
                begin_lat,
                avg_speed,
                event_categories,
                coal_dock_id,
                begin_year as year,
                begin_month as month,
                begin_quarter as quarter
            FROM
                {vars.warehouse_schema}.{tbn.all_stop_events}
            WHERE
                begin_year IN ({year_filter_str})
                and coal_dock_id is not null
            """
        ,
        con=vars.engine
    )
    return _df


def load_events_without_dock(year: int, vars: ShoreNetVariablesManager) -> pd.DataFrame:
    """
    load events without dock
    Args:
        year: int, year of events
        vars: ShoreNetVariablesManager, framework variables

    Returns:
        Pandas DataFrame
    """
    _df = pd.read_sql(
        sql=f"""
            SELECT
                event_id,
                mmsi,
                begin_time,
                end_time,
                end_time - begin_time as duration,
                begin_lng,
                begin_lat,
                avg_speed,
                event_categories,
                coal_dock_id,
                begin_year as year,
                begin_month as month,
                begin_quarter as quarter
            FROM
                {vars.warehouse_schema}.{tbn.all_stop_events}
            WHERE
                begin_year = {year} and coal_dock_id is null
            """
        ,
        con=vars.engine
    )
    return _df


def load_dock_polygon(vars: ShoreNetVariablesManager) -> list[dict]:
    """
    get dock polygon from sql server

    Args:
        vars: ShoreNetVariablesManager, framework variables

    Returns:
        a list of polygons details
    """
    Session = sessionmaker(bind=vars.engine)
    session = Session()
    polygons_query = session.query(
        DimDockPolygon.Id,
        DimDockPolygon.Name,
        func.ST_AsText(DimDockPolygon.Polygon).label('Polygon'),
        DimDockPolygon.lng,
        DimDockPolygon.lat,
        DimDockPolygon.type_id
    ).order_by(
        DimDockPolygon.Id
    )
    
    polygons = polygons_query.all()

    dock_polygon_list = []
    for polygon in polygons:
        wkt_polygon = polygon.Polygon
        pattern = re.compile(r'\d+\.\d+\s\d+\.\d+')
        matches = pattern.findall(wkt_polygon)
        coordinates = [[float(coord) for coord in match.split()] for match in matches]
        dock_polygon_list.append(
            {
                'dock_id': polygon.Id,
                'name': polygon.Name,
                'polygon': coordinates,
                'lng': polygon.lng,
                'lat': polygon.lat,
                'type_id': polygon.type_id
            }
        )
    # print(f"Dock polygon count: {len(dock_polygon_list)}")
    session.close()
    # dock_polygon_df = pd.DataFrame(dock_polygon_list)
    return dock_polygon_list


def load_csv_dock_polygon(file_path: str) -> list[dict]:
    """
    load dock polygon from csv file

    Args:
        file_path: str, path to the csv file

    Returns:
        a list of polygons details
    """
    dock_polygon_df = pd.read_csv(file_path)
    dock_polygon_list = []
    for _, row in dock_polygon_df.iterrows():
        _polygon_points = eval(row['polygon'])
        dock_polygon_list.append(
            {
                'dock_id': row['dock_id'],
                'name': row['name'],
                'polygon': _polygon_points,
                'lng': _polygon_points[0][1],
                'lat': _polygon_points[0][0]
            }
        )
    return dock_polygon_list


def load_od_pairs(year: int, vars: ShoreNetVariablesManager) -> pd.DataFrame:
    """
    load od pairs

    Args:
        vars: ShoreNetVariablesManager, framework variables
        year: int, year of events

    Returns:
        Pandas DataFrame
    """
    _df = pd.read_sql(
        sql=f"""
            SELECT
                departure_dock_id,
                arrival_dock_id,
                departure_year,
                departure_month,
                arrival_year,
                arrival_month,
                sail_duration
            FROM
                {vars.warehouse_schema}.{tbn.data_od_pairs}
            WHERE
                departure_year = {year}
            """
        ,
        con=vars.engine
    )
    return _df

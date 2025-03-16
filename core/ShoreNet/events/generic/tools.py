"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  tools.py
@DateTime:  27/10/2024 4:47 pm
@DESC    :  events tools
"""

import re

import pandas as pd

from sqlalchemy import func, or_, text
from sqlalchemy.orm import sessionmaker

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.infrastructure.definition.parameters import Prefix
from core.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon
from core.infrastructure.definition.parameters import WarehouseDefinitions as tbn


def load_events_all(year: int, month: int, vars: ShoreNetVariablesManager) -> pd.DataFrame:
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
    with vars.engine.connect() as conn:
        df = pd.read_sql(
            sql=text(query), con=conn
        )
    return df


def load_events_with_dock(year: int, vars: ShoreNetVariablesManager) -> pd.DataFrame:
    """
    load events with dock
    :param year: year condition
    :return: dataframe
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
                begin_year = {year} and coal_dock_id is not null
            """
        ,
        con=vars.engine
    )
    return _df


def load_events_without_dock(year: int, vars: ShoreNetVariablesManager) -> pd.DataFrame:
    """
    load events with dock
    :param year: year condition
    :return: dataframe
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


def load_dock_polygon(vars: ShoreNetVariablesManager) -> pd.DataFrame:
    """
    get dock polygon from sql server
    :return: [{'dock_id': ..., 'name': ..., 'polygon': [...], 'province': ... }]
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


def load_od_pairs(year: int, vars: ShoreNetVariablesManager) -> pd.DataFrame:
    """
    load od pairs
    :param year: year condition
    :return: dataframe
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

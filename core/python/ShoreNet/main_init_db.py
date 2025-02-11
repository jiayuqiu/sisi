# -*- encoding: utf-8 -*-
'''
@File    :   main_init_db.py
@Time    :   2025/02/11 20:23:57
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   init database
'''

import os
from sqlalchemy import text
from sqlalchemy import inspect

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.utils.db.base import Base
from core.ShoreNet.utils.db.DataODPairs import DataODPairs
from core.ShoreNet.utils.db.DimDockPolygon import DimDockPolygon
from core.ShoreNet.utils.db.DimPolygonType import DimPolygonType
from core.ShoreNet.utils.db.DimShipsStatics import DimShipsStatics
from core.ShoreNet.utils.db.FactorAllStopEvent import FactorAllStopEvents
from core.ShoreNet.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app():
    vars = VariablesManager()

    # -. check database if exists
    with vars.engine.connect() as con:
        result = con.execute(
            text("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = :db_name"),
            {"db_name": os.environ["SISI_DB_DATABASE"]}
        )
    
    if not result.scalar():
        vars.engine.execute(text(f"CREATE DATABASE {os.environ['SISI_DB_DATABASE']}"))
        _logger.info(f"Database '{os.environ['SISI_DB_DATABASE']}' created successfully.")
    else:
        _logger.info(f"Database '{os.environ['SISI_DB_DATABASE']}' already existed.")
    
    # -. check tables
    inspector = inspect(vars.engine)
    existing_tables = inspector.get_table_names()
    
    tables_to_create = [
        cls.__tablename__
        for cls in Base.__subclasses__()
        if cls.__tablename__ not in existing_tables
    ]
    
    if tables_to_create:
        print(f"Creating tables: {tables_to_create}")
        Base.metadata.create_all(vars.engine)
        print("Tables created successfully!")
    else:
        print("All tables already exist. No new tables were created.")
    

if __name__ == "__main__":
    run_app()
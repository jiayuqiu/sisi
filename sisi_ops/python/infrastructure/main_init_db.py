# -*- encoding: utf-8 -*-
'''
@File    :   main_init_db.py
@Time    :   2025/02/11 20:23:57
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   init database
'''

import argparse
from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager
from sisi_ops.infrastructure.definition.parameters import ArgsDefinition as Ad
from sisi_ops.utils.db.o2m.ShoreNet.base import Base
from sisi_ops.utils.db.o2m.ShoreNet.DimDockPolygon import DimDockPolygon
from sisi_ops.utils.db.o2m.ShoreNet.FactorAllStopEvent import FactorAllStopEvents
from sisi_ops.utils.db.o2m.ShoreNet.DataODPairs import DataODPairs
from sisi_ops.utils.db.o2m.ShoreNet.DimPolygonType import DimPolygonType
from sisi_ops.utils.db.o2m.ShoreNet.DimShipsStatics import DimShipsStatics
from sisi_ops.utils.db.definitions.sqlite import SQLiteConfig
from sisi_ops.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app():
    parser = argparse.ArgumentParser(description='process init database tables')
    parser.add_argument(f"--{Ad.force_flag}", action="store_true", help='if force to init database')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    args = parser.parse_args()
    stage_env = args.__getattribute__(Ad.stage_env)
    force = args.__getattribute__(Ad.force_flag)
    
    vars = ShoreNetVariablesManager(stage_env)
    if vars.db_type == "sqlite":
        # db_config = SQLiteConfig(db_path=vars.db_path)
        # db_config.create_db_file()
        raise NotImplementedError("SQLite database initialization is not implemented yet.")
    elif vars.db_type == "mysql":
        # raise NotImplementedError("MySQL database initialization is not implemented yet.")
        # -. check database if exists
        with vars.engine.connect() as con:
            result = con.execute(
                text("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = :db_name"),
                {"db_name": vars.warehouse_schema}
            )

        if not result.scalar():
            raise RuntimeError(
                f"Database '{vars.warehouse_schema}' not found. Please create the database first."
            )
        else:
            _logger.info(f"Database '{vars.warehouse_schema}' already existed.")
    else:
        raise ValueError(f"Unsupported database type: {vars.db_type}")


    
    # -. check tables
    inspector = inspect(vars.engine)
    existing_tables = inspector.get_table_names()

    # -. if force is true, drop tables first
    if force:
        Base.metadata.drop_all(vars.engine)
        existing_tables = []
    
    tables_to_create = [
        cls.__tablename__
        for cls in Base.__subclasses__()
        if cls.__tablename__ not in existing_tables
    ]
    
    if tables_to_create:
        Base.metadata.create_all(vars.engine)
        _logger.info("Tables created successfully!")
    else:
        _logger.info("All tables already exist. No new tables were created.")
    

if __name__ == "__main__":
    run_app()
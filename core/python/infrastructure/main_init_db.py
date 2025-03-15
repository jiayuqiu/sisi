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
from sqlalchemy import text
from sqlalchemy import inspect

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.ShoreNet.definitions.parameters import ArgsDefinition as Ad
from core.ShoreNet.utils.db.base import Base
from core.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app():
    parser = argparse.ArgumentParser(description='process init database tables')
    parser.add_argument(f"--{Ad.force_flag}", action="store_true", help='if force to init database')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    args = parser.parse_args()
    stage_env = args.__getattribute__(Ad.stage_env)
    force = args.__getattribute__(Ad.force_flag)
    
    vars = ShoreNetVariablesManager(stage_env)

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
        _logger.info(f"Creating tables: {tables_to_create}")
        Base.metadata.create_all(vars.engine)
        _logger.info("Tables created successfully!")
    else:
        _logger.info("All tables already exist. No new tables were created.")
    

if __name__ == "__main__":
    run_app()
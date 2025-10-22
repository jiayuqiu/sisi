"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  variables.py
@DateTime:  20/10/2024 3:03 pm
@DESC    :  set variables manager
"""

import os
import platform
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import OperationalError


from sisi_ops.ShoreNet.conf import connect_database
from sisi_ops.infrastructure.definition.parameters import (
    Prefix,
    DirPathNames,
    FileNames,
    EventFilterParameters,
    GeoParameters,
    WarehouseDefinitions,
    ColumnNames
)


class ShoreNetVariablesManager:
    def __init__(self, stage_env: str):
        load_dotenv("./.env")
        self.stage_env = stage_env
        self.root_path = os.environ["ROOT_PATH"]
        
        # dir path
        self.dp_names: DirPathNames = self.define_dir_path()
        os_name = platform.system()
        if os.name == 'nt' or os_name == 'Windows':
            for key, value in self.dp_names.__dict__.items():
                self.dp_names.__dict__[key] = value.replace('/', '\\')

        # file names
        self.f_names: FileNames = self.define_file_names()

        # table names
        self.table_names: WarehouseDefinitions = self.define_warehouse()

        # column names
        self.column_names: ColumnNames = self.define_column_names()

        # parameters
        self.event_param = EventFilterParameters()
        self.geo_param = GeoParameters()
        self.dbscan_params = {  # NOTE: for dummy data, eps is 10km and min_samples is 5. please increase both of them for real data
            'eps': 10 / self.geo_param.kms_per_radian,
            'min_samples': 5
        }

        # MULTIPLE PROCESS WORKERS SETTINGS
        self.process_workers = 16

        # TODO: add loading of parameters from config file(yml)

        # connect to database
        self.db_engine = connect_database(stage_env)
        self.warehouse_schema = f"{Prefix.sisi}{self.stage_env}"

    def define_dir_path(self) -> DirPathNames:
        return DirPathNames()

    def engine(self) -> Engine:
        if self.db_engine is None:
            raise ValueError("database connection failed")
        else:
            try:
                self.db_engine.connect()
            except OperationalError as e:
                if "Unknown database" in str(e):
                    raise ConnectionError(f"database connection failed. error: {e}, please create database first.")
                else:
                    raise ConnectionError(f"database connection failed. error: {e}")
        return self.db_engine

    @staticmethod
    def define_file_names() -> FileNames:
        return FileNames()

    @staticmethod
    def define_warehouse() -> WarehouseDefinitions:
        return WarehouseDefinitions()

    @staticmethod
    def define_column_names() -> ColumnNames:
        return ColumnNames()
    
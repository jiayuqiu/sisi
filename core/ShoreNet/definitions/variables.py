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


from core.ShoreNet.conf import connect_database
from core.ShoreNet.definitions.parameters import (
    DirPathNames,
    FileNames,
    EventFilterParameters,
    GeoParameters,
    TableNames,
    ColumnNames
)


class VariablesManager:
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
        self.table_names: TableNames = self.define_table_names()

        # column names
        self.column_names: ColumnNames = self.define_column_names()

        # parameters
        self.event_param = EventFilterParameters()
        self.geo_param = GeoParameters()
        self.dbscan_params = {
            'eps': 0.2 / self.geo_param.kms_per_radian,
            'min_samples': 30
        }

        # MULTIPLE PROCESS WORKERS SETTINGS
        self.process_workers = 2

        # TODO: add loading of parameters from config file(yml)
        # connect to database
        self.engine = connect_database(stage_env)

        if self.engine is None:
            raise ValueError("database connection failed")
        else:
            try:
                self.engine.connect()
            except sqlalchemy.exc.OperationalError as e:
                if "Unknown database" in str(e):
                    raise ConnectionError(f"database connection failed. error: {e}, please create database first.")
                else:
                    raise ConnectionError(f"database connection failed. error: {e}")

    def define_dir_path(self) -> DirPathNames:
        return DirPathNames(
            output_path='output'
        )

    @staticmethod
    def define_file_names() -> FileNames:
        return FileNames()

    @staticmethod
    def define_table_names() -> TableNames:
        return TableNames()

    @staticmethod
    def define_column_names() -> ColumnNames:
        return ColumnNames()
    
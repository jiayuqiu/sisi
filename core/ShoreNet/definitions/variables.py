"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  variables.py
@DateTime:  20/10/2024 3:03 pm
@DESC    :  set variables manager
"""

import os
from typing import Union

from core.ShoreNet.conf import get_data_path
from core.ShoreNet.conf import connect_database
from core.ShoreNet.definitions.parameters import (
    DirPathNames,
    FileNames,
    EventFilterParameters,
    GeoParameters,
    TableNames
)


class VariablesManager:
    def __init__(self):
        self.data_path: str = get_data_path()
        self.dp_names: DirPathNames = self.define_dir_path()
        self.f_names: FileNames = self.define_file_names()
        self.table_names: TableNames = self.define_table_names()
        self.event_param = EventFilterParameters()
        self.geo_param = GeoParameters()

        self.dbscan_params = {
            'eps': 0.2 / self.geo_param.kms_per_radian,
            'min_samples': 30
        }

        # TODO: add loading of parameters from config file(yml)
        # connect to database
        self.engine = connect_database()

        if self.engine is None:
            raise ValueError("database connection failed")
        else:
            try:
                self.engine.connect()
            except Exception as e:
                raise ConnectionError(f"database connection failed. error: {e}")

    def define_dir_path(self) -> DirPathNames:
        return DirPathNames(
            ship_statics_path=os.path.join(self.data_path, 'statics'),
            output_path=os.path.join('./', 'output')
        )

    @staticmethod
    def define_file_names() -> FileNames:
        return FileNames()

    @staticmethod
    def define_table_names() -> TableNames:
        return TableNames()

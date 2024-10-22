"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  variables.py
@DateTime:  20/10/2024 3:03 pm
@DESC    :  set variables manager
"""

import os
from typing import Union

from sqlalchemy.engine.base import Engine

from core.ShoreNet.conf import get_data_path
from core.ShoreNet.conf import connect_database
from core.ShoreNet.definitions.parameters import (
    DirPathNames,
    FileNames,
    DBSCANParameters,
    EventFilterParameters,
    GeoParameters
)


class VariablesManager:
    def __init__(self):
        self.data_path: str = get_data_path()
        self.dp_names: DirPathNames = self.define_dir_path()
        self.f_names: FileNames = self.define_file_names()
        self.event_param = EventFilterParameters()
        self.geo_param = GeoParameters()

        # TODO: add loading of parameters from config file(yml)
        self.dbscan_param = DBSCANParameters(
            eps=0.2/self.geo_param.kms_per_radian,
            min_samples=30
        )

        # connect to database
        self.engine: Union[Engine, None] = connect_database()

        if self.engine is None:
            raise ValueError("database connection failed")
        else:
            try:
                self.engine.connect()
            except Exception as e:
                raise ConnectionError(f"database connection failed. error: {e}")

    def define_dir_path(self) -> DirPathNames:
        return DirPathNames(
            ship_statics_path=os.path.join(self.data_path, 'statics')
        )

    @staticmethod
    def define_file_names() -> FileNames:
        return FileNames()

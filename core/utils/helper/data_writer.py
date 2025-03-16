# -*- encoding: utf-8 -*-
"""
@File    :   data_writer.py
@Time    :   2025/03/16 12:58:18
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
"""

from typing import Any, Union
from abc import ABC, abstractmethod

from pandas.core.frame import DataFrame as PandasDF
from sqlalchemy import text

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm
from core.utils.setup_logger import set_logger

_logger = set_logger(__name__)


class DataWriter(ABC):
    def __init__(self, vars: Vm, data: PandasDF, table_name: str, key_args: Union[dict[str, Any], None]):
        self.vars = vars
        self.data = data
        self.table_name = table_name
        self.key_args = key_args

    @abstractmethod
    def insert(self) -> None:
        pass

    @abstractmethod
    def update(self, ) -> None:
        pass

    @abstractmethod
    def delsert(self) -> None:
        pass


class PandasWriter(DataWriter):
    def __init__(self, vars: Vm, data: PandasDF, table_name: str, key_args: Union[dict[str, Any], None]):
        super().__init__(vars, data, table_name, key_args)

    def insert(self) -> None:
        """This function is to insert data into database

        steps:
        1. through
        """

    def update(self) -> None:
        pass

    def delsert(self) -> None:
        """This function is to delete and insert data into database.

        steps:
        1. based on key_args, delete data from database
            - key_args's structure example:
                {"mmsi": 123456, "begin_time": 1234567890}
            so, will delete data where mmsi=123456 and begin_time=1234567890
        2. through pandas to_sql method, insert data into database
            - pandas to_sql arguments:
                - con -> self.vars.engine
                - table_name -> self.table_name
                - if_exists -> 'append'
                - index -> False

        Args:
            data (PandasDF): _description_
            key_cols (list[str]): _description_
        """
        # Step 1: Deletion based on key_args
        if self.key_args:
            # Build SQL condition string with placeholders
            condition = " AND ".join(f"{col} = :{col}" for col in self.key_args.keys())
            delete_query = f"DELETE FROM {self.table_name} WHERE {condition}"
            with self.vars.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(delete_query), self.key_args)
        else:
            # No key_args provided; proceeding with insertion only.
            _logger.warning(
                msg="No key arguments provided. Proceeding with data insertion only. " +
                    "To delete existing data before insertion, supply valid key_args or use the 'insert' method instead."
            )

        # Step 2: Insert data using pandas to_sql method
        self.data.to_sql(
            name=self.table_name,
            con=self.vars.engine,
            if_exists='append',
            index=False
        )
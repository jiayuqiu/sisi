# -*- encoding: utf-8 -*-
"""
@File    :   data_writer.py
@Time    :   2025/03/16 12:58:18
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
"""

import os
import sys
from typing import Any, Union
from abc import ABC, abstractmethod
from tqdm import tqdm
import tempfile

import pandas as pd
from pandas.core.frame import DataFrame as PandasDF
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from sisi_ops.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm
from sisi_ops.infrastructure.definition.parameters import (
    LibraryVariableNames as Lvn
)
from sisi_ops.utils.helper.tools import timer
from sisi_ops.utils.setup_logger import set_logger

_logger = set_logger(__name__)


class DataWriter(ABC):
    def __init__(self, vars: Vm, data: PandasDF, table_name: str, orm_class, key_args: Union[dict[str, Any], None]):
        self.vars = vars
        self.data = data
        self.table_name = table_name
        self.key_args = key_args
        self.orm_class = orm_class

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
    def __init__(self, vars: Vm, data: PandasDF, table_name: str, orm_class, key_args: Union[dict[str, Any], None]):
        super().__init__(vars, data, table_name, orm_class, key_args)
        # self.data.rename(columns=CN_MAPPING, inplace=True)

    @timer
    def insert(self, chunksize=10000) -> None:
        """This function is to insert data into database
        """
        # Build mapping from DB column name to ORM attribute name.
        col_mapping = {col.name: col.key for col in self.orm_class.__table__.columns}

        # Retrieve custom attribute mapping if defined.
        attribute_map = {}
        if Lvn.attribute_name_mapping in self.orm_class.__dict__.keys():
            attribute_map = self.orm_class.__getattribute__(self.orm_class, Lvn.attribute_name_mapping)

        total_rows = self.data.shape[0]
        num_chunks = (total_rows + chunksize - 1) // chunksize
        Session = sessionmaker(bind=self.vars.engine)
        
        for start in tqdm(range(0, total_rows, chunksize), total=num_chunks, desc="Inserting chunks"):
            end = min(total_rows, start + chunksize)
            chunk = self.data.iloc[start:end]
            # Create a new session from engine
            try:
                session = Session()
                for _, row in chunk.iterrows():
                    insert_row = {}
                    # For each key in the row, if it exists in DB mapping, use the custom mapping if available.
                    for k, v in row.items():
                        if v is None:
                            continue
                        if pd.isnull(v):
                            continue
                        if k in col_mapping:
                            target_key = attribute_map.get(k, col_mapping[k])
                            insert_row[target_key] = v
                    orm_instance = self.orm_class(**insert_row)
                    session.add(orm_instance)
                session.commit()
            except Exception as e:
                session.rollback()
                _logger.error(f"Failed to insert chunk rows {start} to {end}: {e}")
                sys.exit(1)
            finally:
                session = Session()
    
    @timer
    def mysql_insert(self) -> None:
        """Inserts data into the database using MySQL's LOAD DATA LOCAL INFILE for speed.

        This process:
        1. Renames/reorders DataFrame columns to match the database table.
        2. Exports the entire DataFrame to a temporary CSV file.
        3. Executes the LOAD DATA LOCAL INFILE statement.
        """
        # Build mapping from DB column name to ORM attribute name.
        # (col.name is the actual name in the table; col_mapping is used for reference)
        col_mapping = {col.name: col.key for col in self.orm_class.__table__.columns}

        # Retrieve custom attribute mapping if defined.
        attribute_map = {}
        if Lvn.attribute_name_mapping in self.orm_class.__dict__.keys():
            attribute_map = self.orm_class.__getattribute__(self.orm_class, Lvn.attribute_name_mapping)

        # Prepare a copy of the original DataFrame.
        df = self.data.copy()
        
        # Rename DataFrame columns (if custom mapping exists, override the default DB column name).
        # Here we assume the source DataFrame columns are the same as keys in col_mapping.
        rename_dict = {}
        for db_col, orm_attr in col_mapping.items():
            # If a custom mapping exists for the source column name,
            # then we want the target (DB) column to be as defined in the ORM.
            # For example, if attribute_map defines {"begin_year": "year"}, then source column "begin_year"
            # should be renamed to "begin_year" (which is already the DB column name) – in many cases this step
            # is optional if your DataFrame columns already match the DB column names.
            source_col = orm_attr  # default source column is the ORM attribute name
            if source_col in attribute_map:
                # If custom mapping exists, use the ORM column (which is the DB column name)
                source_col = attribute_map.get(source_col, source_col)
            # In any case, we want the DataFrame column renamed to the DB column name.
            if source_col in df.columns:
                rename_dict[source_col] = db_col

        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)

        # Retain only the columns that are present in the table (and order them as in the table).
        db_columns = [col.name for col in self.orm_class.__table__.columns if col.name in df.columns]
        df = df[db_columns]

        # Replace None or NaN with MySQL's NULL representation in CSV, here using "\N"
        df.fillna(r"\N", inplace=True)

        # Write to a temporary CSV file.
        tmp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix=".csv")
        try:
            # Write file as tab-separated values.
            df.to_csv(tmp_file.name, sep="\t", header=False, index=False)
            tmp_file.close()

            # Build the LOAD DATA LOCAL INFILE query.
            columns_list = ", ".join(f"`{col}`" for col in db_columns)
            load_query = text(f"""
                LOAD DATA LOCAL INFILE :file_path
                INTO TABLE {self.table_name}
                FIELDS TERMINATED BY '\\t'
                LINES TERMINATED BY '\\n'
                ({columns_list})
                """)

            # Connect and execute the LOAD DATA query with local_infile enabled.
            with self.vars.engine.connect().execution_options(local_infile=True) as conn:
                trans = conn.begin()
                try:
                    conn.execute(load_query, {"file_path": tmp_file.name})
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    _logger.error(f"Failed to load data from file {tmp_file.name}: {e}")
                    raise e

        except Exception as e:
            _logger.error(f"Error during MySQL load file process: {e}")
            raise e
        finally:
            if os.path.exists(tmp_file.name):
                os.remove(tmp_file.name)

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
            print(f"Data deleted from table '{self.table_name}' based on key arguments.")
        else:
            # No key_args provided; proceeding with insertion only.
            _logger.warning(
                msg="No key arguments provided. Proceeding with data insertion only. " +
                    "To delete existing data before insertion, supply valid key_args or use the 'insert' method instead."
            )

        # Step 2: Insert data using pandas to_sql method
        # self.mysql_insert()
        self.insert()

from abc import ABC, abstractmethod

import pandas as pd
from pyspark.sql import DataFrame


class BaseReader(ABC):
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def read_table(self, table_path: str) -> pd.DataFrame | DataFrame:
        raise TypeError(
            f"Base reader has no functionality for `read_table`"
        )
    
    @abstractmethod
    def read_file(self, file_path: str) -> pd.DataFrame | DataFrame:
        raise TypeError(
            f"Base reader has no functionality for `read_file`"
        )

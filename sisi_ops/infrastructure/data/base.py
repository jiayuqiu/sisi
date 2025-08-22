# -*- encoding: utf-8 -*-
"""
@File    :   base.py
@Time    :   2025/03/02 14:43:33
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
"""

import os
import pandas as pd
from abc import ABC, abstractmethod

from pandas.core.frame import DataFrame as PandasDF


def column_validate():
    pass


class BaseDataProcessor(ABC):
    def __init__(self, csv_file):
        self.csv_file = csv_file
        if os.path.exists(csv_file):
            self.df = pd.read_csv(csv_file)
        else:
            raise FileNotFoundError(f"File {csv_file} not found.")

    @abstractmethod
    def clean_up(self, data: PandasDF) -> PandasDF:
        pass

    @abstractmethod
    def preprocess(self, data: PandasDF) -> PandasDF:
        pass
    
    @abstractmethod
    def wrangle(self, year: int, month: int) -> PandasDF:
        pass

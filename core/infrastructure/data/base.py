# -*- encoding: utf-8 -*-
"""
@File    :   base.py
@Time    :   2025/03/02 14:43:33
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
"""

from abc import ABC, abstractmethod


class BaseDataProcessor(ABC):
    def __init__(self, csv_file):
        self.csv_file = csv_file
    
    @abstractmethod
    def clean_up(self):
        pass

    @abstractmethod
    def filter_null_error(self):
        pass
    
    @abstractmethod
    def wrangle(self,):
        pass

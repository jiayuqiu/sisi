# -*- encoding: utf-8 -*-
"""
@File    :   events.py
@Time    :   2025/03/02 14:34:39
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   upload events proecessor
"""

import pandas as pd

from core.infrastructure.data.base import BaseDataProcessor


class EventsDataProcessor(BaseDataProcessor):
    def __init__(self, csv_file):
        super().__init__(csv_file)
    
    def clean_up(self):
        print("i am EventsDataProcess instance.")
        return super().clean_up()
    
    def wrangle(self):
        return super().wrangle()


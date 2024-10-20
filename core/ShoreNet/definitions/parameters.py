"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  parameters.py
@DateTime:  20/10/2024 3:16 pm
@DESC    :  for analysis parameters
"""

from dataclasses import dataclass


@dataclass
class FileNames:
    ship_statics_fn: str = "coal_mmsi_v1_init_static.csv"
    ship_dwt_fn: str = "coal_mmsi_v1_init.csv"


@dataclass
class DirPathNames:
    ship_statics_path: str

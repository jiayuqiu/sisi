"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  parameters.py
@DateTime:  20/10/2024 3:16 pm
@DESC    :  for analysis parameters
"""

from dataclasses import dataclass


@dataclass
class GeoParameters:
    kms_per_radian: float = 6371.0088


@dataclass
class DBSCANParameters:
    eps: float
    min_samples: int


@dataclass
class EventFilterParameters:
    stop_duration_min: int = 1800
    stop_duration_max: int = 7 * 24 * 3600
    event_category: str = "stop_event_poly"


@dataclass
class FileNames:
    ship_statics_fn: str = "coal_mmsi_v1_init_static.csv"
    ship_dwt_fn: str = "coal_mmsi_v1_init.csv"


@dataclass
class DirPathNames:
    ship_statics_path: str

"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  parameters.py
@DateTime:  20/10/2024 3:16 pm
@DESC    :  for analysis parameters
"""

from dataclasses import dataclass


@dataclass
class MultiProcessWorkers:
    process_workers: int = 8


@dataclass
class GeoParameters:
    kms_per_radian: float = 6371.0088


@dataclass
class EventFilterParameters:
    stop_duration_min: int = 1800
    stop_duration_max: int = 7 * 24 * 3600
    event_category: str = "stop_event_poly"
    cluster_unique_mmsi_min_count: int = 200  # means the cluster should have at least 200 unique ships
    cluster_events_min_count: int = 400 # means the cluster should have at least 400 events

    # lng_field_name: str = "Begin_lon"
    # lat_field_name: str = "Begin_lat"
    # use_fields = [
    #     'mmsi', 'Begin_time', 'End_time', lng_field_name, lat_field_name, 'avgSpeed', 'Point_num',
    #     'Event_categories'
    # ]

    event_lng_range = [73, 136]
    event_lat_range = [18, 50]
    event_avg_speed_max = 0.72


@dataclass
class TableNames:
    all_stop_events_table_name: str = "factor_all_stop_events"
    coal_stop_events_table_name: str = "factor_coal_stop_events"
    dim_ships_statics_table_name: str = "dim_ships_statics"


@dataclass
class ColumnNames:
    lng_column_name: str = "begin_lng"
    lat_column_name: str = "begin_lat"

@dataclass
class FileNames:
    ship_statics_fn: str = "coal_mmsi_v1_init_static.csv"
    ship_dwt_fn: str = "coal_mmsi_v1_init.csv"


@dataclass
class DirPathNames:
    ship_statics_path: str
    output_path: str
    test_analyze_source_data_path: str = "tests/shared_data/analyze/source"
    test_analyze_result_data_path: str = "tests/shared_data/analyze/result"

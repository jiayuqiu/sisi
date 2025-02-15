"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  parameters.py
@DateTime:  20/10/2024 3:16 pm
@DESC    :  for analysis parameters
"""

from dataclasses import dataclass


@dataclass
class Prefix:
    sisi: str = "sisi_"


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

    event_lng_range = [73, 136]
    event_lat_range = [18, 50]
    event_avg_speed_max = 0.72
    
    polygon_event_max_distance = 500  # NOTE: 500 km is for dummy data, if real data, should be 50 km


@dataclass
class TableNames:
    all_stop_events_table_name: str = "factor_all_stop_events"
    coal_stop_events_table_name: str = "factor_coal_stop_events"
    dim_ships_statics_table_name: str = "dim_ships_statics"
    data_od_pairs_table_name: str = "data_od_pairs"
    data_features_trust_score_month_table_name: str = "data_features_trust_score_month"
    data_features_trust_score_quarter_table_name: str = "data_features_trust_score_quarter"


@dataclass
class ColumnNames:
    lng_column_name: str = "begin_lng"
    lat_column_name: str = "begin_lat"

@dataclass
class FileNames:
    ship_statics_fn: str = "statics.csv"
    ship_dwt_fn: str = "statics_dwt.csv"


@dataclass
class DirPathNames:
    output_path: str
    test_analyze_source_data_path: str = "tests/shared_data/analyze/source"
    test_analyze_result_data_path: str = "tests/shared_data/analyze/result"
    test_utils_data_path: str = "tests/shared_data/utils"
    root_path: str = "./"
    data_path: str = "./data"


@dataclass
class StageName:
    dummy: str = "dummy"
    prod: str = "prod"
    test: str = "test"


@dataclass
class ArgsDefinition:
    stage_env: str = "stage_env"
    year: str = "year"
    start_month: str = "start_month"
    end_month: str = "end_month"
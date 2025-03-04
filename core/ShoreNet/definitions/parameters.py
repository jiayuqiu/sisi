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
    
    # NOTE: means the cluster should have at least 200 unique ships
    #       for dummy data, should be 1, for real data, should be 200
    cluster_unique_mmsi_min_count: int = 1
    
    # NOTE: means the cluster should have at least 400 events
    #       for dummy data, should be 1, for real data, should be 400
    cluster_events_min_count: int = 1

    event_lng_range = [73, 136]
    event_lat_range = [18, 50]
    event_avg_speed_max = 0.72
    
    polygon_event_max_distance = 500  # NOTE: 500 km is for dummy data, if real data, should be 50 km


@dataclass
class WarehouseDefinitions:
    all_stop_events: str = "factor_all_stop_events"
    coal_stop_events: str = "factor_coal_stop_events"
    dim_ships_statics: str = "dim_ships_statics"
    data_od_pairs: str = "data_od_pairs"
    data_features_trust_score_month: str = "data_features_trust_score_month"
    data_features_trust_score_quarter: str = "data_features_trust_score_quarter"


@dataclass
class ColumnNames:
    mmsi: str = "mmsi"
    lng: str = "begin_lng"
    lat: str = "begin_lat"
    sog: str = "avg_speed"
    year: str = "begin_year"
    month: str = "begin_month"
    day: str = "begin_day"

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
    force_flag: str = "force"
    stage_env: str = "stage_env"
    year: str = "year"
    start_month: str = "start_month"
    end_month: str = "end_month"
    
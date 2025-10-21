"""
@Author:    Jerry Qiu
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
class Suffix:
    var: str = "_var"

@dataclass
class CoordinatePrecision:
    precision: int = 6


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
    timestamp: str = "begin_time"
    lng: str = "begin_lng"
    lat: str = "begin_lat"
    sog: str = "avg_speed"
    year: str = "begin_year"
    month: str = "begin_month"
    day: str = "begin_day"
    date_id: str = "date_id"

    # statics column names
    statics_data_timestamp: str = "receivetime"
    statics_data_length: str = "length"
    statics_data_width: str = "width"
    ship_name: str = "ship_name"
    ship_type: str = "ship_type"
    length_width_ratio: str = "length_width_ratio"
    dwt: str = "dwt"

    # events column names
    events_data_timestamp: str = timestamp


@dataclass
class FileNames:
    ship_statics_fn: str = "statics.csv"
    ship_dwt_fn: str = "statics_dwt.csv"


@dataclass
class DirPathNames:
    output_path: str = "./data/output"
    test_analyze_source_data_path: str = "tests/shared_data/analyze/source"
    test_analyze_result_data_path: str = "tests/shared_data/analyze/result"
    test_utils_data_path: str = "tests/shared_data/utils"
    root_path: str = "./"
    data_path: str = "./data"
    statics_folder_name: str = "statics"
    events_folder_name: str = "events"
    kml_folder_name: str = "kml"


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
    data_type: str = "data_type"
    polygon_fn: str = "polygon_fn"
    reset_flag: str = "reset_flag"


@dataclass
class StaticsCleanThreshold:
    """Threshold for statics data clean up

    How the ratio(min_ratio_threshold & max_ratio_threshold) comes:
    # *. calculate length_width_ratio which indicates the shape of the ship.
    # if length_width_ratio is too sharp, it may be an error data.
    # set ±5% as confidence interval to filter error data.
    static_df.loc[:, 'length_width_ratio'] = static_df.apply(lambda row: row['length'] / row['width'], axis=1)

    - `min_ratio_threshold`: minimum threshold for length_width_ratio
        calculate fomula: np.percentile(static_df['length_width_ratio'], 5)

    - `max_ratio_threshold`: maximum threshold for length_width_ratio
        calculate fomula: np.percentile(static_df['length_width_ratio'], 95)
    
    3 and 7.1 are the result of the above fomula, based on 2023 data.
    based on the data, the ratio should be updated.
    """
    min_ratio_threshold: float = 3
    max_ratio_threshold: float = 7.1


@dataclass
class LibraryVariableNames:
    # orm class variables
    attribute_name_mapping: str = "attribute_name_mapping"

"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  filter.py
@DateTime:  22/10/2024 9:56 pm
@DESC    :  filter error events
"""
import pandas as pd
from pandas.core.frame import DataFrame

from core.ShoreNet.definitions.variables import VariablesManager


def print_clean_up_effect(original_df: DataFrame, cleaned_df: DataFrame) -> None:
    """
    print the effect of clean up
    :param original_df: original event dataframe
    :param cleaned_df: cleaned event dataframe
    :return: None
    """
    print(f"original event data shape: {original_df.shape}")
    print(f"cleaned event data shape: {cleaned_df.shape}")


def clean_up_events(
        df: DataFrame,
        var: VariablesManager,
        mmsi_enum_list: list[int] = []
) -> DataFrame:
    """
    clean up error events data

    Valid Data Request:
    1. stop duration should greater than 1800 seconds &
       stop duration should less than 7 * 24 * 3600 seconds

    2. events category should be equal to "stop_event_poly"


    :param df: event dataframe
    :param var: variables manager
    :param mmsi_enum_list: mmsi enum list, events mmsi should be in this list.
    :return: cleaned event dataframe
    """
    _df = df.copy()

    # *. filter stop duration and event category
    _df = _df.loc[
        (_df['duration'] > var.event_param.stop_duration_min) &
        (_df['duration'] < var.event_param.stop_duration_max)
        # (_df['event_categories'] == var.event_param.event_category)
    ]

    # *. if there is mmsi_enum_list, then filter mmsi
    if len(mmsi_enum_list) > 0:
        _df = _df.loc[_df['mmsi'].isin(mmsi_enum_list)]
        print_clean_up_effect(df, _df)
        return _df
    else:
        # if there is no mmsi_enum_list, return _df directly
        print_clean_up_effect(df, _df)
        return _df


# def csv_fields_mapping(df: DataFrame, fields_map: dict[str, str]) -> DataFrame:
#     """
#     through fields_map clean events column names
#
#     :param df: dataframe loaded from csv directly
#     :param fields_map: mapping variables
#     :return: cleaned event dataframe
#     """
#     df.rename(columns=fields_map, inplace=True)
#     return df

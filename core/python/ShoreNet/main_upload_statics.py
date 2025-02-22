"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  main_upload_statics.py
@DateTime:  20/10/2024 2:45 pm
@Description: This script is used to upload statics data to the database
"""

import os
import argparse

import pandas as pd

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.definitions.parameters import ArgsDefinition as Ad
from core.basis.setup_logger import set_logger
from core.ShoreNet.statics.filter import clean_up_statics

_logger = set_logger(__name__)

def run_app() -> None:
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    args = parser.parse_args()
    stage_env = args.__getattribute__(Ad.stage_env)
    
    var = VariablesManager(stage_env)

    # -. get DWT
    ship_dwt_df = pd.read_csv(
        os.path.join(
            var.dp_names.data_path,
            stage_env, "statics",
            var.f_names.ship_dwt_fn
        )
    )
    ship_dwt_df.loc[:, 'DWT_float'] = ship_dwt_df['DWT'].str.replace(',', '').astype(float)
    ship_dwt_df = ship_dwt_df[ship_dwt_df['DWT_float'] > 0]

    # -. get statics
    ship_statics_df = pd.read_csv(
        os.path.join(var.dp_names.data_path, stage_env, "statics", var.f_names.ship_statics_fn)
    )
    _logger.info(f"static data shape before cleaning: {ship_statics_df.shape}")
    ship_statics_df = ship_statics_df.rename(
        columns={
            "shipname": "ship_name",
            "shiptype": "ship_type",
            "breadth": "width",
            "DWT_float": "dwt"
        }
    )

    # -. clean statics
    cleaned_statics_df = clean_up_statics(ship_statics_df)
    _logger.info(f"static data shape after cleaning: {cleaned_statics_df.shape}")

    # -. merge with dwt
    cleaned_statics_df = pd.merge(cleaned_statics_df, ship_dwt_df.loc[:, ["mmsi", "DWT_float"]], on='mmsi', how='left')

    # -. upload statics to database
    _logger.info("uploading statics data to database...")
    # cleaned_statics_df = cleaned_statics_df.loc[:, ["mmsi", "ship_name", "ship_type", "length", "breadth", "DWT_float"]]
    # cleaned_statics_df = cleaned_statics_df.rename(
    #     columns={
    #         "shipname": "ship_name",
    #         "shiptype": "ship_type",
    #         "breadth": "width",
    #         "DWT_float": "dwt"
    #     }
    # )
    cleaned_statics_df = cleaned_statics_df.drop_duplicates(subset=['mmsi'], keep="first")
    cleaned_statics_df.to_sql("dim_ships_statics", con=var.engine, if_exists='replace', index=False)
    _logger.info("uploaded statics data to database successfully")


if __name__ == '__main__':
    run_app()

"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  main_upload_statics.py
@DateTime:  20/10/2024 2:45 pm
@Description: This script is used to upload statics data to the database
"""

import os

import pandas as pd

from core.ShoreNet.definitions.variables import VariablesManager
from core.ShoreNet.utils.setup_logger import set_logger
from core.ShoreNet.statics.filter import clean_up

_logger = set_logger(__name__)

def run_app() -> None:
    var = VariablesManager()

    # -. get DWT
    ship_dwt_df = pd.read_csv(
        os.path.join(var.dp_names.ship_statics_path, var.f_names.ship_dwt_fn)
    )
    ship_dwt_df.loc[:, 'DWT_float'] = ship_dwt_df['DWT'].str.replace(',', '').astype(float)
    ship_dwt_df = ship_dwt_df[ship_dwt_df['DWT_float'] > 0]

    # -. get statics
    ship_statics_df = pd.read_csv(
        os.path.join(var.dp_names.ship_statics_path, var.f_names.ship_statics_fn)
    )
    _logger.info(f"static data shape before cleaning: {ship_statics_df.shape}")

    # -. clean statics
    cleaned_statics_df = clean_up(ship_statics_df)
    _logger.info(f"static data shape after cleaning: {cleaned_statics_df.shape}")

    # -. merge with dwt
    cleaned_statics_df = pd.merge(cleaned_statics_df, ship_dwt_df.loc[:, ["mmsi", "DWT_float"]], on='mmsi', how='left')

    # -. upload statics to database
    _logger.info("uploading statics data to database...")
    cleaned_statics_df = cleaned_statics_df.loc[:, ["mmsi", "shipname", "shiptype", "length", "breadth", "DWT_float"]]
    cleaned_statics_df.rename(
        {
            "shipname": "ship_name",
            "ship_type": "ship_type",
            "breadth": "width",
            "DWT_float": "dwt"
        }
    )
    cleaned_statics_df.to_sql("dim_ships_statics", con=var.engine, if_exists='replace', index=False)
    _logger.info("uploaded statics data to database successfully")


if __name__ == '__main__':
    run_app()

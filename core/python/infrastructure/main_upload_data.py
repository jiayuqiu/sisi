# -*- encoding: utf-8 -*-
'''
@File    :   main_upload_data.py
@Time    :   2025/02/22 00:43:27
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

import os
import argparse

from core.infrastructure.data.statics import StaticsDataProcessor
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm
from core.ShoreNet.definitions.parameters import ArgsDefinition as Ad
from core.utils.setup_logger import set_logger

_logger = set_logger(__name__)


def run_app():
    parser = argparse.ArgumentParser(description='process: upload events')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    parser.add_argument(f'--{Ad.year}', type=int, required=True, help='Process year')
    parser.add_argument(f'--{Ad.start_month}', type=int, required=True, help='The start month')
    parser.add_argument(f'--{Ad.end_month}', type=int, required=True, help='The end month')

    args = parser.parse_args()

    stage_env = args.__getattribute__(Ad.stage_env)
    year = args.__getattribute__(Ad.year)
    start_month = args.__getattribute__(Ad.start_month)
    end_month = args.__getattribute__(Ad.end_month)
    
    vars = Vm(stage_env)

    for month in range(start_month, end_month+1):
        month_str = f"{year}{month:02}"
        _logger.info(f"{month_str} events processing...")
        sd_processor = StaticsDataProcessor(
            os.path.join(
                vars.dp_names.data_path, stage_env, 'statics', f"static_{month_str}.csv"
            )
        )
        df = sd_processor.load()
        df.to_sql("dim_ships_statics", con=vars.engine, if_exists='append', index=False)


if __name__ == "__main__":
    run_app()
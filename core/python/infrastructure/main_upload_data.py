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
import glob
import argparse
from typing import Any, Union

from core.infrastructure.data.statics import StaticsDataProcessor
from core.infrastructure.data.events import EventsDataProcessor
# from core.infrastructure.data.polygons import PolygonsDataProcessor
from core.ShoreNet.definitions.variables import ShoreNetVariablesManager as Vm
from core.ShoreNet.utils.db.FactorAllStopEvent import FactorAllStopEvents
from core.ShoreNet.utils.db.DimShipsStatics import DimShipsStatics
from core.infrastructure.definition.parameters import (
    DirPathNames as Dpn,
    ColumnNames as Cn, 
    ArgsDefinition as Ad
)
from core.utils.setup_logger import set_logger
from core.utils.helper.data_writer import PandasWriter

_logger = set_logger(__name__)


def trigger_data_processor(vars: Vm,
                           data_processor: Union[StaticsDataProcessor, EventsDataProcessor], 
                           year: int, 
                           month: int) -> None:
    if isinstance(data_processor, StaticsDataProcessor):
        processor_flag = Dpn.statics_folder_name
        table_cls = DimShipsStatics
    elif isinstance(data_processor, EventsDataProcessor):
        processor_flag = Dpn.events_folder_name
        table_cls = FactorAllStopEvents
    else:
        raise ValueError(f"Invalid data processor -> {data_processor}")
    
    _logger.info(f"{year}-{month:02} {processor_flag} processing...")
    data = data_processor.wrangle(year=year, month=month)
    data.rename(
        columns={
            Cn.year: table_cls.year.name,
            Cn.month: table_cls.month.name
        },
        inplace=True
    )
    data_write = PandasWriter(
        vars=vars,
        data=data,
        table_name=table_cls.__tablename__,
        orm_class=table_cls,
        key_args={table_cls.year.name: year, table_cls.month.name: month}
    )
    _logger.info(f"{year}-{month:02} {processor_flag} : table -> {table_cls.__tablename__}, delserting...")
    data_write.delsert()


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

        # 1. process statics data
        statiacs_data_fp = os.path.join(
            vars.dp_names.data_path, stage_env, Dpn.statics_folder_name, f"{month_str}.csv"
        )
        if os.path.exists(statiacs_data_fp):
            _logger.info(f"{month_str} statics data found, process...")
            statices_processor = StaticsDataProcessor(statiacs_data_fp)
            trigger_data_processor(
                vars=vars,
                data_processor=statices_processor,
                year=year,
                month=month
            )
        else:
            _logger.warning(f"{month_str} statics data not found, skip processing...")

        # 2. process events data
        events_data_fp = os.path.join(
            vars.dp_names.data_path, stage_env, Dpn.events_folder_name, f"{month_str}.csv"
        )
        if os.path.exists(events_data_fp):
            _logger.info(f"{month_str} events data found, process...")
            events_processor = EventsDataProcessor(events_data_fp)
            trigger_data_processor(
                vars=vars,
                data_processor=events_processor,
                year=year,
                month=month
            )
        else:
            _logger.warning(f"{month_str} events data not found, skip processing...")
            continue

        # # 3. NOTE: skip. if need to process polygon data, comment out this part
        # _logger.info(f"{month_str} polygon processing...")
        # kml_fn_ls = glob.glob(os.path.join(vars.dp_names.data_path, stage_env, "kml", '*.kml'))
        # for kml_fn in kml_fn_ls:
        #     parsed_kml_ls = PolygonsDataProcessor(kml_fn=kml_fn).get_polygon_detail()
    print("Done")
        


if __name__ == "__main__":
    run_app()
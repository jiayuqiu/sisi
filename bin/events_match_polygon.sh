source .venv/bin/activate

python -m sisi_ops.python.ShoreNet.main_map_events_polygons --stage_env=dev --year=2020 --start_month=4 --end_month=6 --reset_flag=true --polygon_fn=/home/jerry/codebase/sisi/data/dev/tmp/码头多边形最新.csv

python -m sisi_ops.python.ShoreNet.main_map_events_polygons --stage_env=dev --year=2024 --start_month=4 --end_month=6 --reset_flag=true --polygon_fn=/home/jerry/codebase/sisi/data/dev/tmp/码头多边形最新.csv
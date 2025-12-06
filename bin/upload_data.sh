# conda init
source ./venv/bin/activate

main_upload_data --stage_env=dev --year=2020 --start_month=4 --end_month=6 --data_type=events
main_upload_data --stage_env=dev --year=2024 --start_month=4 --end_month=6 --data_type=events
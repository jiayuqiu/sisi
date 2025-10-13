# ShoreNet Module

This document provides information about the ShoreNet module commands and their usage.

## Available Commands

- **main_map_events_polygons**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--year` (int, required): Data year.
    - `--start_month` (int, required): Data is start from this month.
    - `--end_month` (int, required): Data is end up at this month.
    - `--reset_flag` (bool, optional): Reset flag.
  - **Example:**
    ```bash
    python -m sisi_ops.python.ShoreNet.main_map_events_polygons --stage_env=dev --year=2023 --start_month=2 --end_month=12 --reset_flag=true
    ```

- **main_mapping_od_paris**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--year` (int, required): Data year.
  - **Example:**
    ```bash
    python -m sisi_ops.python.ShoreNet.main_mapping_od_paris --stage_env=dev --year=2023
    ```

- **main_map_ais_polygons**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--year` (int, required): Data year.
    - `--start_month` (int, required): Data is start from this month.
    - `--end_month` (int, required): Data is end up at this month.
  - **Example:**
    ```bash
    python -m sisi_ops.python.ShoreNet.main_map_ais_polygons --stage_env=dev --year=2023 --start_month=1 --end_month=12
    ```

- **main_upload_statics**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
  - **Example:**
    ```bash
    python -m sisi_ops.python.ShoreNet.main_upload_statics --stage_env=dev
    ```

- **main_upload_polygon**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
  - **Example:**
    ```bash
    python -m sisi_ops.python.ShoreNet.main_upload_polygon --stage_env=dev
    ```

- **main_upload_events**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--year` (int, required): Data year.
    - `--start_month` (int, required): Data is start from this month.
    - `--end_month` (int, required): Data is end up at this month.
  - **Example:**
    ```bash
    python -m sisi_ops.python.ShoreNet.main_upload_events --stage_env=dev --year=2023 --start_month=1 --end_month=12
    ```

- **main_dbscan_events**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--year` (int, required): Data year.
    - `--start_month` (int, required): Data is start from this month.
    - `--end_month` (int, required): Data is end up at this month.
  - **Example:**
    ```bash
    python -m sisi_ops.python.ShoreNet.main_dbscan_events --stage_env=dev --year=2023 --start_month=1 --end_month=12
    ```

## VS Code Launch Configurations

You can also run these commands using the predefined VS Code launch configurations:

1. **main_map_events_polygons**
   ```json
   {
       "name": "main_map_events_polygons",
       "type": "debugpy",
       "request": "launch",
       "module": "sisi_ops.python.ShoreNet.main_map_events_polygons",
       "args": ["--stage_env=dev", "--year=2023", "--start_month=2", "--end_month=12", "--reset_flag=true"]
   }
   ```

2. **main_od_pairs**
   ```json
   {
       "name": "main_od_pairs",
       "type": "debugpy",
       "request": "launch",
       "module": "sisi_ops.python.ShoreNet.main_mapping_od_paris",
       "args": ["--stage_env=dev", "--year=2023"]
   }
   ```

## Note

All commands follow the Python standards according to [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide.

- **main_init_db**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--force` (flag, optional): Set to true if provided.
  - **Example:**
    ```bash
    python -m sisi_ops.python.infrastructure.main_init_db --stage_env=dev [--force]
    ```


- **main_upload_data**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--year` (int, required): data year.
    - `--start_month` (int, required): data is start from this month.
    - `--end_month` (int, requeired): data is end up at this month. 
  
  - **Example:**
    ```bash
    # upload Jan 2023 data sets
    python -m sisi_ops.python.infrastructure.main_init_db --stage_env=dev --year=2023 --start_month=1 --end_month=1
    ```

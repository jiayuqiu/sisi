- **main_init_db**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--force` (flag, optional): Set to true if provided.
  - **Example:**
    ```bash
    python -m core.python.ShoreNet.main_init_db --stage_env=dummy [--force]
    ```


- **main_update_data**
  - **Argument Parsing:**
    - `--stage_env` (str, required): Process stage environment.
    - `--year` (int, required): data year.
    - `--start_month` (int, required): data is start from this month.
    - `--end_month` (int, requeired): data is end up at this month. 
  
  - **Example:**
    ```bash
    
    ```

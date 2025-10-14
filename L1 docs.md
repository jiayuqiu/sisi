# SISI Project: Level 1 Documentation

## Overview

The SISI project is a Python-based system that analyzes shipping transportation networks with a focus on:
- Identifying coal docks in China
- Generating coal ships' MMSI lists
- Analyzing dock transportation indicators
- Mapping clustered routes among docks

The system relies heavily on point-in-polygon operations to match ship events with dock locations.

## Infrastructure Setup

### 1. Database Initialization

```bash
# Initialize the database with required tables
python -m sisi_ops.python.infrastructure.main_init_db --stage_env=dev [--force]
```

**Purpose:** This command creates all necessary tables in the MySQL database using SQLAlchemy ORM, including:
- Dimension tables for dock polygons
- Dimension tables for ship statistics
- Fact tables for events and OD pairs

**Arguments:**
- `--stage_env` (str, required): Process stage environment (e.g., dev, prod)
- `--force` (flag, optional): Set to true if provided to force recreation of tables

### 2. Upload Data

```bash
# Upload Jan 2023 data sets
python -m sisi_ops.python.infrastructure.main_upload_data --stage_env=dev --year=2023 --start_month=1 --end_month=1
```

**Purpose:** Upload events, statics, and polygons data to the database.

**Arguments:**
- `--stage_env` (str, required): Process stage environment
- `--year` (int, required): Data year
- `--start_month` (int, required): Data is start from this month
- `--end_month` (int, required): Data is end up at this month

### 3. Environment Configuration

Create a `.env` file in the project root with:

```
SISI_DB_TYPE=mysql
SISI_DB_HOST=127.0.0.1
SISI_DB_PORT=3306
SISI_DB_USER=your_username
SISI_DB_PASSWORD=your_password

DATA_PATH=/your/data/path
ROOT_PATH=/your/project/path
TEST_STAGE_ENV=dev
```

## ShoreNet Operations Workflow

### Step 1: Upload Ship Statistics Data

```bash
# Upload ship statistics data
python -m sisi_ops.python.ShoreNet.main_upload_statics --stage_env=dev
```

**Purpose:** Upload static ship data (MMSI, ship type, dimensions, etc.) to the database.

**Arguments:**
- `--stage_env` (str, required): Process stage environment

**Example Use Case:** Load the ship registry data to identify coal carriers and their characteristics.

---

### Step 2: Upload Polygon Data

```bash
# Upload polygon data from KML files
python -m sisi_ops.python.ShoreNet.main_upload_polygon --stage_env=dev
```

**Purpose:** Upload polygon data representing docks to the database. Polygons are typically created in Google Earth Pro and saved as KML files.

**Arguments:**
- `--stage_env` (str, required): Process stage environment

**Data Source:** KML files stored in `data/{stage_env}/kml/` directory

---

### Step 3: Upload Events Data

```bash
# Upload events data (ship movements)
python -m sisi_ops.python.ShoreNet.main_upload_events --stage_env=dev --year=2023 --start_month=1 --end_month=12
```

**Purpose:** Upload ship events data (AIS positions, timestamps, status) to the database.

**Arguments:**
- `--stage_env` (str, required): Process stage environment
- `--year` (int, required): Data year
- `--start_month` (int, required): Data is start from this month
- `--end_month` (int, required): Data is end up at this month

**Example:**
```bash
# Upload events for Q1 2023
python -m sisi_ops.python.ShoreNet.main_upload_events --stage_env=dev --year=2023 --start_month=1 --end_month=3
```

---

### Step 4: Match Events to Polygons

```bash
# Map events to dock polygons
python -m sisi_ops.python.ShoreNet.main_map_events_polygons --stage_env=dev --year=2023 --start_month=2 --end_month=12 --reset_flag=true
```

**Purpose:** Match event data with polygon data to identify dock stops. This is the core point-in-polygon operation.

**Arguments:**
- `--stage_env` (str, required): Process stage environment
- `--year` (int, required): Data year
- `--start_month` (int, required): Data is start from this month
- `--end_month` (int, required): Data is end up at this month
- `--reset_flag` (bool, optional): Reset flag to clear previous assignments

**Process:**
1. Loads event data from the database
2. Loads polygon definitions from KML files or database
3. Uses a point-in-polygon algorithm to determine which dock (if any) each event occurred in
4. Updates the database with the polygon assignments

**Performance Note:** The matching process uses either:
- Standard Python implementation
- Optimized Cython implementation for 10x speed improvement

---

### Step 5: DBSCAN Clustering for Identifying New Dock Locations

```bash
# Run DBSCAN clustering on events without polygon assignments
python -m sisi_ops.python.ShoreNet.main_dbscan_events --stage_env=dev --year=2023 --start_month=1 --end_month=12
```

**Purpose:** Cluster events with DBSCAN algorithm to identify potential dock locations where no polygon exists.

**Arguments:**
- `--stage_env` (str, required): Process stage environment
- `--year` (int, required): Data year
- `--start_month` (int, required): Data is start from this month
- `--end_month` (int, required): Data is end up at this month

**Process:**
1. Identifies events without polygon assignments
2. Applies DBSCAN clustering algorithm using geographic distance
3. Creates convex hulls around clusters to define new polygon boundaries
4. Outputs new polygon definitions for manual review

**Output:** New polygon definitions that can be imported into Google Earth Pro or directly into the database.

---

### Step 6: Map AIS Data to Polygons (Alternative)

```bash
# Match AIS data with polygon data
python -m sisi_ops.python.ShoreNet.main_map_ais_polygons --stage_env=dev --year=2023 --start_month=1 --end_month=12
```

**Purpose:** Match AIS data with polygon data (alternative to events-based matching).

**Arguments:**
- `--stage_env` (str, required): Process stage environment
- `--year` (int, required): Data year
- `--start_month` (int, required): Data is start from this month
- `--end_month` (int, required): Data is end up at this month

---

### Step 7: Calculate Origin-Destination Pairs

```bash
# Calculate OD pairs based on event sequences
python -m sisi_ops.python.ShoreNet.main_mapping_od_paris --stage_env=dev --year=2023
```

**Purpose:** Calculate origin-destination pairs for shipping routes based on sequential dock visits.

**Arguments:**
- `--stage_env` (str, required): Process stage environment
- `--year` (int, required): Data year

**Process:**
1. Analyzes the sequence of dock visits for each ship
2. Identifies origin and destination pairs
3. Calculates trip statistics (duration, distance, frequency)
4. Stores OD pair data in the database

**Output:** OD pairs that form the basis of the shipping network analysis and route clustering.

---

## Complete Workflow Example

Here's a complete workflow for processing 2023 data:

```bash
# Step 1: Initialize database (first time only)
python -m sisi_ops.python.infrastructure.main_init_db --stage_env=dev --force

# Step 2: Upload static ship data
python -m sisi_ops.python.ShoreNet.main_upload_statics --stage_env=dev

# Step 3: Upload dock polygon definitions
python -m sisi_ops.python.ShoreNet.main_upload_polygon --stage_env=dev

# Step 4: Upload ship events for entire year
python -m sisi_ops.python.ShoreNet.main_upload_events --stage_env=dev --year=2023 --start_month=1 --end_month=12

# Step 5: Match events to polygons
python -m sisi_ops.python.ShoreNet.main_map_events_polygons --stage_env=dev --year=2023 --start_month=1 --end_month=12 --reset_flag=true

# Step 6: Identify new dock locations from unmatched events
python -m sisi_ops.python.ShoreNet.main_dbscan_events --stage_env=dev --year=2023 --start_month=1 --end_month=12

# Step 7: Calculate origin-destination pairs
python -m sisi_ops.python.ShoreNet.main_mapping_od_paris --stage_env=dev --year=2023
```

## VS Code Debug Configurations

The project includes pre-configured VS Code launch configurations in `.vscode/launch.json`:

### Infrastructure Configurations

**main_init_db:**
```json
{
    "name": "main_init_db",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.infrastructure.main_init_db",
    "args": ["--stage_env=dev", "--force"]
}
```

**main_upload_data:**
```json
{
    "name": "main_upload_data",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.infrastructure.main_upload_data",
    "args": ["--stage_env=dev", "--year=2023", "--start_month=10", "--end_month=12"]
}
```

### ShoreNet Configurations

**main_map_events_polygons:**
```json
{
    "name": "main_map_events_polygons",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.ShoreNet.main_map_events_polygons",
    "args": ["--stage_env=dev", "--year=2023", "--start_month=2", "--end_month=12", "--reset_flag=true"]
}
```

**main_od_pairs:**
```json
{
    "name": "main_od_pairs",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.ShoreNet.main_mapping_od_paris",
    "args": ["--stage_env=dev", "--year=2023"]
}
```

## Key Technical Components

### Geographic Distance Calculation

The `get_geodist` function implements the haversine formula to calculate the great-circle distance between two points on Earth's surface:

```python
def get_geodist(lon1, lat1, lon2, lat2):
    """
    Calculate the great-circle distance between two geographic points
    on the Earth's surface using the haversine formula.
    
    Args:
        lon1 (float): Longitude of the first point
        lat1 (float): Latitude of the first point
        lon2 (float): Longitude of the second point
        lat2 (float): Latitude of the second point
    
    Returns:
        float: Distance between the two points in kilometers
    """
```

This function is essential for:
- DBSCAN clustering to identify nearby events
- Calculating trip distances
- Validating polygon assignments

### Point-in-Polygon Detection

The system uses polygon definitions from KML files to determine whether a ship's location falls within a dock's boundaries.

**Key modules:**
- `sisi_ops.ShoreNet.utils.polygon.py`: KML parser and polygon operations
- `sisi_ops.cython.geo_cython.pyx`: Optimized Cython implementation

**Algorithm:** Ray-casting or winding number algorithm for efficient point-in-polygon detection.

### Polygon Processing Workflow

1. Parse polygon definitions from KML files using the `KMLParser` class
2. Convert polygons to the appropriate format for database storage
3. Match ship events against polygons to identify dock visits
4. For locations without defined polygons, use DBSCAN clustering to identify potential new docks
5. Generate convex hulls around clusters to create new polygon definitions
6. Export new polygons for validation and integration

## Data Storage Structure

```
data/
├── dev/                    # Development environment
│   ├── events/            # Ship events data
│   ├── kml/               # Polygon KML files
│   ├── statics/           # Ship static data
│   └── tmp/               # Temporary files
├── dummy/                 # Test/dummy data
│   ├── ais/
│   ├── events/
│   ├── kml/
│   └── statics/
└── output/                # Analysis results
    ├── demand_mmsi.json   # Ship MMSI lists
    └── csv/               # Export files
```

## Best Practices

Following the project's coding guidelines (from `.github/copilot-instructions.md`):

1. **No hardcoded values** should be used in the code
2. Use **f-strings** for string formatting
3. Follow the [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide for Python code
4. Reference structure details from `.github/resources/structure_details.md`

### Code Standards

```python
# Good: Using f-strings and no hardcoded values
stage_env = config.STAGE_ENV
file_path = f"{config.DATA_PATH}/{stage_env}/events"

# Bad: Hardcoded values and old string formatting
file_path = "/home/user/data/dev/events"
message = "Processing %s environment" % stage_env
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors:**
   - Verify `.env` file configuration
   - Check MySQL service is running
   - Validate credentials

2. **Missing KML Files:**
   - Ensure KML files are in `data/{stage_env}/kml/` directory
   - Check file naming conventions
   - Validate KML file structure

3. **Point-in-Polygon Performance:**
   - Use Cython-optimized version for large datasets
   - Consider batching events for processing
   - Add spatial indexes to database

4. **Memory Issues with Large Datasets:**
   - Process data in monthly chunks
   - Use pandas chunking for file reading
   - Consider upgrading to PySpark for scalability

## Testing

The project includes unit tests in the `tests/` directory:

```bash
# Run all tests
pytest tests/

# Run specific module tests
pytest tests/ShoreNet/
pytest tests/infrastructure/
```

## Documentation

### Sphinx Documentation

Generate API documentation:

```bash
cd docs
conda run -n sisi make html
```

View documentation at: `docs/_build/html/index.html`

### Module Documentation

- Infrastructure Module: [sisi_ops/python/infrastructure/README.md](./sisi_ops/python/infrastructure/README.md)
- ShoreNet Module: [sisi_ops/python/ShoreNet/README.md](./sisi_ops/python/ShoreNet/README.md)


## Support and Contribution

For questions or contributions, please refer to the main [README.md](./README.md) for project overview and development status.

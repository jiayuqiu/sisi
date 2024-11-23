# SISI

## 1 Overview

### 1.1 Transportation Network
This project focuses on:
- Generate coal docks in China.
- Generate coal ships' mmsi list.
- Analyze docks transportation indicators including average and transportation capability, transprotation duration to different docks, shipowner analysis and etc.
- Clustered route among docks.

The analysis is conducted using Python, leveraging libraries such as Pandas, NumPy, MySQL and etc for efficient data manipulation, and Matplotlib or Poltly for visualization.

## 2 Entry

### 2.1 upload data
```bash
# -. upload data
$ python core.python.main_upload_statics.py  # update statics data
$ python core.python.main_upload_events.py --year=2023 --start_month=1 --end_month=12  # update events data
```

### 2.2 map events to polygons
```bash
# This script will take a long time to run. based on the size of events data.
# If you have a powerful cpu, please increase the number of `MultiProcessWorkers.process_workers` in `core.ShoreNet.definitions.parameters`.
$ python core.python.main_map_events_poygons.py --year=2023 --start_month=1 --end_month=12
```

### 2.3 map od pairs
```bash
$ python core.python.main_mapping_od_paris.py --year=2023
```

### 2.4 

## 3. Now Researching

### 3.1 Trust Score of Polygon

#### Status:
Not clear. Still can not find the right way to calculate the trust score of polygon.

#### Pre-Processed:

1. dbscan clustering on each polygon.
2. after filtering out noise events, effectual cluster of stop events are left.

#### Have tried:

1. calculate the density of each dbscan-ed cluster. density = event_count / mmsi_count
2. departure ship count and arrival ship count of each polygon, in month level and quarter level.
3. calculate the percentage of stop events in each dbscan-ed cluster.

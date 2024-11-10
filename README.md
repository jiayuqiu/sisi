# SISI

## 1 Overview

### 1.1 Transportation Network
This project focuses on:
- Generate coal docks in China.
- Generate coal ships' mmsi list.
- Analyze docks transportation indicators including average and transportation capability, transprotation duration to different docks, shipowner analysis and etc.
- Clustered route among docks.

The analysis is conducted using Python, leveraging libraries such as Pandas, NumPy, MySQL and etc for efficient data manipulation, and Matplotlib or Poltly for visualization.

## 2 run main function

### 2.1 upload data
```bash
# -. upload data
$ python core.python.main_upload_statics.py  # update statics data
$ python core.python.main_upload_events.py --year=2023 --start_month=1 --end_month=12  # update events data
```

### 2.2 map events to polygons
```bash
$ python core.python.main_map_events_poygons.py --year=2023 --start_month=1 --end_month=12
```

### polygon

1. trust polygon
2. logistic network, line size refer to cargo volume.
3. 2023 event.
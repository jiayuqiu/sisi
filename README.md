# SISI

## 1 Overview

### 1.1 Transportation Network
This project focuses on:
- Generate coal docks in China.
- Generate coal ships' mmsi list.
- Analyze docks transportation indicators including average and transportation capability, transprotation duration to different docks, shipowner analysis and etc.
- Clustered route among docks.

The analysis is conducted using Python, leveraging libraries such as Pandas, NumPy, MySQL and etc for efficient data manipulation, and Matplotlib or Poltly for visualization.

## 2 Project Structure
The project is organized into the following directories and files:

```
.
├── bin
├── core
│     ├── ShoreNet
│     │     ├── definitions
│     │     ├── demos
│     │     ├── outputs
│     │     ├── scripts
│     │     ├── statics
│     │     └── utils
│     ├── notebooks
│     │     └── html
│     └── python
├── docs
└── logs
```

## 3 run main function

### 3.1 upload statics data
```bash
# -. upload statics data
$ python core.python.main_upload_statics.py
```
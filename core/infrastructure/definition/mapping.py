"""
@File    :   mapping.py
@Time    :   2025/02/23 14:44:31
@Author  :   jerry
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :
"""


# add: destination, eta, etc.
# format: shipname(uppercase)
# keep: utc_ts, imo, callsign, 
# drop: `""`
STATICS_COLUMNS_MAPPING = {
    "mmsi": "mmsi",
    "imo": "imo",
    "receivetime": "receivetime",
    "callsign": "callsign",
    "shipname": "ship_name",
    "shiptype": "ship_type",
    "length": "length",
    "breadth": "width"
}
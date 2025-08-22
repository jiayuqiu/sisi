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


EVENT_FIELDS_MAPPING = {
    "Event_id": "event_id",
    "mmsi": "mmsi",
    "Mmsi_cate": "mmsi_cate",
    "Ship_name": "ship_name",
    "IMO": "imo",
    "Begin_time": "begin_time",
    "End_time": "end_time",
    "Begin_lon": "begin_lng",
    "Begin_lat": "begin_lat",
    "end_lon": "end_lng",
    "end_lat": "end_lat",
    "Middle_lon": "middle_lng",
    "Middle_lat": "middle_lat",
    "Avg_lon": "avg_lng",
    "Avg_lat": "avg_lat",
    "Middle_hdg": "middle_hdg",
    "Max_hdg": "max_hdg",
    "Min_hdg": "min_hdg",
    "Middle_sog": "middle_sog",
    "Max_sog": "max_sog",
    "Middle_cog": "middle_cog",
    "Max_cog": "max_cog",
    "Min_cog": "min_cog",
    "Max_rot": "max_rot",
    "Min_rot": "min_rot",
    "Middle_rot": "middle_rot",
    "Point_num": "point_num",
    "avgSpeed": "avg_speed",
    "AvgSteadySpeed": "avg_steady_speed",
    "SailingDist": "sailing_distance",
    "Zone_id": "zone_id",
    "Navistate": "navistate",
    "nowPortName": "now_port_name",
    "nowPortId": "now_port_id",
    "nowDockName": "now_dock_name",
    "nowDockId": "now_dock_id",
    "nowBerthName": "now_berth_name",
    "nowBerthId": "now_berth_id",
    "nowPortLon": "now_port_lng",
    "nowPortLat": "now_port_lat",
    "Event_categories": "event_categories",
    "province": "province",
    "Country": "country",
    "Event_cate": "event_cate"
}

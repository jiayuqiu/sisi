"""
@Author： Jerry Qiu
@Email:     qiujiayu0212@gmail.com
@FileName:  tools.py
@DateTime:  20/10/2024 2:48 pm
@DESC    :  for generic functions
"""


import datetime


def get_year_month_day(ts: int) -> [int, int, int]:
    """
    get year, month, day from timestamp
    :param ts: timestamp
    :return: year, month, day
    """
    dt = datetime.datetime.fromtimestamp(ts)
    return dt.year, dt.month, dt.day

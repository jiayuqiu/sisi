"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  tools.py
@DateTime:  23/11/2024 10:04 am
@DESC    :  
"""

import datetime


def get_quarter(timestamp: int) -> int:
    month = datetime.datetime.fromtimestamp(timestamp).month
    if month in [1, 2, 3]:
        return 1
    elif month in [4, 5, 6]:
        return 2
    elif month in [7, 8, 9]:
        return 3
    else:
        return 4

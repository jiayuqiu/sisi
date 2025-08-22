# -*- encoding: utf-8 -*-
'''
@File    :   amap.py
@Time    :   2024/08/09 18:01:42
@Author  :   qiujiayu 
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

# here put the import lib
import pandas as pd
import numpy as np

import requests


def amap_request(lng, lat):
    # https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=<用户的key>&radius=1000&extensions=all
    key = "083c8dacc6cadcd2bd390f59c8057547"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}
    request_url = (f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={lng},{lat}&key={key}&"
                   f"radius=1000&extensions=base")
    response = requests.get(request_url, headers=headers)
    # print(j['regeocode']['addressComponent']['province'], j['regeocode']['addressComponent']['district'])
    return response.json()

"""
地理相关函数
"""
import math
import numpy as np


def point_poly(lng, lat, polygon_points):
    """
    判断点是否在多边形内
    :param lng: 该点的经度，类型：float
    :param lat: 该点的纬度，类型：float
    :param polygon_points: 多边形的经纬度坐标列表，类型：list
    :return: t/f
    """
    polygon = np.array(polygon_points)
    polygon = np.array([[float(x) for x in line] for line in polygon])
    cor = len(polygon)
    i = 0
    j = cor - 1
    inside = False
    while i < cor:
        if ((((polygon[i, 1] < lng) & (polygon[j, 1] >= lng))
             | ((polygon[j, 1] < lng) & (polygon[i, 1] >= lng)))
                & ((polygon[i, 0] <= lat) | (polygon[j, 0] <= lat))):
            a = (polygon[i, 0] +
                 (lng - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) *
                 (polygon[j, 0] - polygon[i, 0]))

            if a < lat:
                inside = not inside
        j = i
        i = i + 1
    return inside


EARTH_RADIUS = 6378.137  # 地球半径，单位千米


def getRadian(x):
    return x * math.pi / 180.0


def get_geodist(lon1, lat1, lon2, lat2):
    """
    get geo distance, return unit: km.
    """
    lon1, lat1 = float(lon1), float(lat1)
    lon2, lat2 = float(lon2), float(lat2)
    radLat1 = getRadian(lat1)
    radLat2 = getRadian(lat2)

    a = radLat1 - radLat2
    b = getRadian(lon1) - getRadian(lon2)

    dst = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                  math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    dst = dst * EARTH_RADIUS
    dst = round(dst * 100000000) / 100000000

    return dst
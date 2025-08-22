"""
地理相关函数
"""
import math
import numpy as np
import numba
from numba import njit, prange


@njit
def point_poly_numba(lng, lat, polygon):
    cor = polygon.shape[0]
    i = 0
    j = cor - 1
    inside = False
    while i < cor:
        if (((polygon[i, 1] < lat and polygon[j, 1] >= lat) or
             (polygon[j, 1] < lat and polygon[i, 1] >= lat)) and
                (polygon[i, 0] <= lng or polygon[j, 0] <= lng)):
            a = polygon[i, 0] + (lat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) * (
                        polygon[j, 0] - polygon[i, 0])
            if a < lng:
                inside = not inside
        j = i
        i += 1
    return inside


@njit(parallel=True)
def point_poly_batch(points, polygon) -> np.ndarray:
    """
    Process multiple points in parallel.
    :param points: numpy array of shape (n, 2) where each row is [lng, lat]
    :param polygon: numpy array for the polygon
    :return: numpy array of booleans
    """
    n_points = points.shape[0]
    results = np.empty(n_points, dtype=np.bool_)
    for i in prange(n_points):
        lng = points[i, 0]
        lat = points[i, 1]
        results[i] = point_poly_numba(lng, lat, polygon)
    return results


def point_poly(lng, lat, polygon_points):
    """
    Check if a point (lng, lat) is inside a polygon.
    :param lng: Longitude (float)
    :param lat: Latitude (float)
    :param polygon_points: List of [lng, lat] pairs
    :return: True if inside, False otherwise
    """
    polygon = np.array(polygon_points)
    cor = len(polygon)
    i = 0
    j = cor - 1
    inside = False
    while i < cor:
        if (((polygon[i, 1] < lat and polygon[j, 1] >= lat) or
             (polygon[j, 1] < lat and polygon[i, 1] >= lat)) and
            (polygon[i, 0] <= lng or polygon[j, 0] <= lng)):
            a = polygon[i, 0] + (lat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) * (polygon[j, 0] - polygon[i, 0])
            if a < lng:
                inside = not inside
        j = i
        i += 1
    return inside


EARTH_RADIUS = 6378.137  # 地球半径，单位千米


def getRadian(x):
    return x * math.pi / 180.0


def get_geodist(lon1, lat1, lon2, lat2):
    """
    Calculate the great-circle distance between two geographic points
    on the Earth's surface using the haversine formula.
    
    The coordinates are provided in decimal degrees.
    
    Args:
        lon1 (float): Longitude of the first point.
        lat1 (float): Latitude of the first point.
        lon2 (float): Longitude of the second point.
        lat2 (float): Latitude of the second point.
    
    Returns:
        float: Distance between the two points in kilometers.
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    distance = EARTH_RADIUS * c

    return distance


@njit
def haversine(lon1, lat1, lon2, lat2):
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0)**2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return EARTH_RADIUS * c

@njit
def point_in_poly(lng, lat, polygon):
    inside = False
    n = polygon.shape[0]
    j = n - 1
    for i in range(n):
        if ((polygon[i, 1] > lat) != (polygon[j, 1] > lat)) and \
           (lng < (polygon[j, 0] - polygon[i, 0]) * (lat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) + polygon[i, 0]):
            inside = not inside
        j = i
    return inside
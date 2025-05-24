import numpy as np
cimport numpy as np

def point_poly_c(double lng, double lat, object polygon_points):
    """
    判断点是否在多边形内

    Args:
        lng (double): 经度
        lat (double): 纬度
        polygon_points: 多边形的经纬度列表，可以为列表或NumPy数组

    Returns:
        bool: 点是否在多边形内
    """
    cdef np.ndarray[double, ndim=2] polygon_arr = np.asarray(polygon_points, dtype=np.float64)
    cdef int cor = polygon_arr.shape[0]
    cdef int i, j
    cdef double a
    cdef bint inside = False

    j = cor - 1
    for i in range(cor):
        if (((polygon_arr[i, 1] < lat and polygon_arr[j, 1] >= lat) or
             (polygon_arr[j, 1] < lat and polygon_arr[i, 1] >= lat)) and
            ((polygon_arr[i, 0] <= lng) or (polygon_arr[j, 0] <= lng))):

            a = polygon_arr[i, 0] + (lat - polygon_arr[i, 1]) / (polygon_arr[j, 1] - polygon_arr[i, 1]) * (polygon_arr[j, 0] - polygon_arr[i, 0])
            if a < lng:
                inside = not inside
        j = i
    return inside
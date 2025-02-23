# -*- encoding: utf-8 -*-
'''
@File    :   parameters.py
@Time    :   2025/02/23 15:49:30
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

from dataclasses import dataclass


@dataclass
class StaticsCleanThreshold:
    """Threshold for statics data clean up

    How the ratio(min_ratio_threshold & max_ratio_threshold) comes:
    # *. calculate length_width_ratio which indicates the shape of the ship.
    # if length_width_ratio is too sharp, it may be an error data.
    # set ±5% as confidence interval to filter error data.
    static_df.loc[:, 'length_width_ratio'] = static_df.apply(lambda row: row['length'] / row['width'], axis=1)

    - `min_ratio_threshold`: minimum threshold for length_width_ratio
        calculate fomula: np.percentile(static_df['length_width_ratio'], 5)

    - `max_ratio_threshold`: maximum threshold for length_width_ratio
        calculate fomula: np.percentile(static_df['length_width_ratio'], 95)
    
    3 and 7.1 are the result of the above fomula, based on 2023 data.
    based on the data, the ratio should be updated.
    """
    min_ratio_threshold: float = 3
    max_ratio_threshold: float = 7.1
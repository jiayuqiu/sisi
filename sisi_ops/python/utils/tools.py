# -*- encoding: utf-8 -*-
'''
@File    :   tools.py
@Time    :   2025/02/11 21:00:23
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   None
'''

from sqlalchemy import inspect


def table_exists(engine, table_name: str) -> bool:
    """check table if exists

    Args:
        engine (_type_): db connect engine
        table_name (str): table_name

    Returns:
        bool: exists return True, other wise return False
    """
    inspector = inspect(engine)
    return inspector.has_table(table_name)

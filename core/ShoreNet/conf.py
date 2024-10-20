import os
import platform
from typing import Union

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def get_data_path():
    """
    get data path, the only hard code.
    :return:
    """
    os_name = platform.system()
    if os.name == 'nt' or os_name == 'Windows':
        data_path = "D:\\data\\sisi\\"
    elif os.name == 'posix' or os_name == 'Linux':
        data_path = r"/mnt/d/data/sisi/"
    else:
        data_path = r"/mnt/d/data/sisi/"

    return data_path


def connect_mysql() -> Engine:
    """
    connect mysql
    :return:
    """
    mysql_properties = {
        "host": os.environ["SISI_DB_HOST"],
        "port": os.environ["SISI_DB_PORT"],
        "database": os.environ["SISI_DB_DATABASE"],
        "user": os.environ["SISI_DB_USER"],
        "password": os.environ["SISI_DB_PASSWORD"]
    }
    mysql_uri = (
        f"mysql+pymysql://{mysql_properties['user']}:{mysql_properties['password']}@{mysql_properties['host']}/"
        f"{mysql_properties['database']}?charset=utf8"
    )
    mysql_engine = create_engine(mysql_uri)
    return mysql_engine


def connect_database(sql_type: str = "mysql") -> Union[Engine, None]:
    """
    connect database

    :param sql_type: mysql, default is mysql.
    :return: sqlalchemy engine
    """
    if sql_type == "mysql":
        _e = connect_mysql()
        return _e
    else:
        return None

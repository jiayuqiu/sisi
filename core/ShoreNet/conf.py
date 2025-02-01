import os

from sqlalchemy import create_engine


def connect_mysql():
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


def connect_database(sql_type: str = "mysql"):
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

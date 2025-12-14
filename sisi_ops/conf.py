import os

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from sisi_ops.infrastructure.definition.parameters import Prefix


def connect_mysql(stage_env: str) -> Engine:
    """
    connect mysql
    :return:
    """
    mysql_properties = {
        "host": os.environ["SISI_DB_HOST"],
        "port": os.environ["SISI_DB_PORT"],
        "database": f"{Prefix.sisi}{stage_env}",
        "user": os.environ["SISI_DB_USER"],
        "password": os.environ["SISI_DB_PASSWORD"]
    }
    mysql_uri = (
        f"mysql+pymysql://{mysql_properties['user']}:{mysql_properties['password']}@{mysql_properties['host']}/"
        f"{mysql_properties['database']}?charset=utf8"
    )
    mysql_engine: Engine = create_engine(mysql_uri, echo=False)
    return mysql_engine


def connect_sqlite(db_path) -> Engine:
    """
    connect sqlite
    :return:
    """
    sqlite_engine: Engine = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 60})
    return sqlite_engine


def connect_database(stage_env: str, sql_type: str = "mysql", db_path: str = "") -> Engine:
    """
    connect database

    :param sql_type: mysql, default is mysql.
    :return: sqlalchemy engine
    """
    if sql_type == "mysql":
        _e = connect_mysql(stage_env)
        return _e
    elif sql_type == "sqlite":
        _e = connect_sqlite(db_path)
        return _e
    else:
        NotImplementedError(f"{sql_type} is not supported.")

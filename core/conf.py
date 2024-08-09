
from sqlalchemy import create_engine

# SQL Server properties
sql_server_properties = {
    "host": "localhost",
    "port": 1433,
    "database": "sisi",
    "user": "sa",
    "password": "Amacs%400212",
    "driver": "ODBC Driver 18 for SQL Server"  # Ensure this matches the driver name exactly
}

mysql_properties = {
    "host": "172.30.250.57",
    "port": 3306,
    "database": "sisi",
    "user": "root",
    "password": "UZcM7g1%40i4",
}

# Construct the connection string
odbc_uri = (
    f"mssql+pyodbc://{sql_server_properties['user']}:{sql_server_properties['password']}@"
    f"{sql_server_properties['host']}:{sql_server_properties['port']}/{sql_server_properties['database']}?"
    f"driver={sql_server_properties['driver'].replace(' ', '+')}&TrustServerCertificate=yes&Encrypt=no"
)

pymssql_uri = (
    f"mssql+pymssql://{sql_server_properties['user']}:{sql_server_properties['password']}@"
    f"{sql_server_properties['host']}@{sql_server_properties['port']}/{sql_server_properties['database']}?charset=utf8"
)

mysql_uri = (
    f"mysql+pymysql://{mysql_properties['user']}:{mysql_properties['password']}@{mysql_properties['host']}/"
    f"{mysql_properties['database']}?charset=utf8"
)

# # Create the SQLAlchemy engine
# ss_engine = create_engine(
#     pymssql_uri,
#     echo=False,
#     # fast_executemany=True,
#     # use_insertmanyvalues=False
# )

mysql_engine = create_engine(mysql_uri)

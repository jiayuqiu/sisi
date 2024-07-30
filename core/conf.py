from sqlalchemy import create_engine

# SQL Server properties
sql_server_properties = {
    "host": "127.0.0.1",
    "port": 1433,
    "database": "sisi",
    "user": "sa",
    "password": "Amacs%400212",
    "driver": "ODBC Driver 18 for SQL Server"  # Ensure this matches the driver name exactly
}

# Construct the connection string
uri = (
    f"mssql+pyodbc://{sql_server_properties['user']}:{sql_server_properties['password']}@"
    f"{sql_server_properties['host']}:{sql_server_properties['port']}/{sql_server_properties['database']}?"
    f"driver={sql_server_properties['driver'].replace(' ', '+')}&TrustServerCertificate=yes&Encrypt=no"
)
# print(uri)

# Create the SQLAlchemy engine
ss_engine = create_engine(
    uri,
    echo=False,
    fast_executemany=True,
    use_insertmanyvalues=False
)

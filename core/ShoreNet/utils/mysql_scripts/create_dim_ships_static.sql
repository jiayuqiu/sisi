DROP TABLE IF EXISTS sisi.dim_ships_static;
CREATE TABLE sisi.dim_ships_statics(
    mmsi      INT,
    ship_name NVARCHAR(255),
    ship_type INT,
    length    INT,
    width     INT,
    clt_time  TIMESTAMP,
    PRIMARY KEY (mmsi, clt_time)
);
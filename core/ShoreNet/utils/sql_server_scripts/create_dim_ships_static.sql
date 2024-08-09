DROP TABLE IF EXISTS sisi.ShoreNet.dim_ships_static;

-- Create the table
CREATE TABLE sisi.ShoreNet.dim_ships_static
(
    mmsi      INT PRIMARY KEY,
    ship_name NVARCHAR(255),
    ship_type INT,
    length    INT,
    width     INT
);

-- Create an index on the ship_type column
CREATE INDEX idx_ship_type ON sisi.ShoreNet.dim_ships_static (ship_type);
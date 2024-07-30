IF NOT EXISTS (SELECT * FROM ShoreNet.tab_dock_polgon WHERE name='PolygonData' AND xtype='U')
CREATE TABLE PolygonData
(
    Id       INT PRIMARY KEY IDENTITY,
    Name     NVARCHAR(255),
    Polygon  GEOMETRY,
    Province NVARCHAR(255)
);
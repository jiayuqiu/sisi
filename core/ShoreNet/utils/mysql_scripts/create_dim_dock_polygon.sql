drop table if exists dim_dock_polygon;
create table dim_dock_polygon
(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name NVARCHAR(255),
    Polygon GEOMETRY NOT NULL,
    Province NVARCHAR(255),
    District NVARCHAR(255),
    lng FLOAT,
    lat FLOAT,
    type_id INT,
    stage_id INT
)
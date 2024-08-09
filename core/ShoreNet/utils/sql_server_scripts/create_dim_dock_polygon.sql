drop table if exists ShoreNet.dim_dock_polygon;
create table ShoreNet.dim_dock_polygon
(
    Id       int identity
        primary key,
    Name     nvarchar(255),
    Polygon  geometry not null,
    Province nvarchar(255),
    Distruct nvarchar(255),
    lng      float,
    lat      float,
    type_id  int,
    stage_id int
)
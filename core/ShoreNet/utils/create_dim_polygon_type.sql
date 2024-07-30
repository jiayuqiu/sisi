CREATE TABLE sisi.ShoreNet.dim_polygon_type
(
    id                    int NOT NULL,
    type_id               int NOT NULL,
    polygon_type_desc_eng nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    polygon_type_sec_chn  nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    CONSTRAINT dim_polygon_type_pk PRIMARY KEY (id)
);
CREATE
NONCLUSTERED INDEX dim_polygon_type_type_id_index ON sisi.ShoreNet.dim_polygon_type (type_id);
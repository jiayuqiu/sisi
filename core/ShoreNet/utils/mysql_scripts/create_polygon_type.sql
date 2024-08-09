DROP TABLE IF EXISTS sisi.dim_polygon_type;
CREATE TABLE sisi.dim_polygon_type(
    id                    INT AUTO_INCREMENT PRIMARY KEY,
    type_id               int NOT NULL,
    polygon_type_desc_eng varchar(255),
    polygon_type_sec_chn  varchar(255),
    INDEX idx_type_id(type_id)
)
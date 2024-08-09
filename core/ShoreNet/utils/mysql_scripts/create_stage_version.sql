DROP TABLE IF EXISTS dim_stage_version;
CREATE TABLE dim_stage_version(
    Id               INT PRIMARY KEY AUTO_INCREMENT,
    stage_id         INT NOT NULL,
    create_time      DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_stage_id(stage_id)
);
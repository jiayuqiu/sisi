DROP TABLE IF EXISTS sisi.factor_coal_stop_events;
CREATE TABLE sisi.factor_coal_stop_events(
    event_id         VARCHAR(50) PRIMARY KEY,
    mmsi             bigint,
    begin_time       bigint,
    end_time         bigint,
    lng              decimal(10, 6),
    lat              decimal(10, 6),
    point_num        int,
    avg_speed        decimal(10, 6),
    event_categories VARCHAR(50),
    coal_dock_id     int,
    create_time      DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_avg_speed(avg_speed, event_categories)
)
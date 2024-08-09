DROP TABLE IF EXISTS sisi.factor_stop_events;
CREATE TABLE sisi.factor_stop_events(
    mmsi             bigint,
    Begin_time       bigint,
    End_time         bigint,
    Middle_lon       decimal(10, 6),
    Middle_lat       decimal(10, 6),
    Point_num        int,
    avgSpeed         decimal(10, 6),
    Event_categories VARCHAR(50),
    coal_dock_id     int,
    create_time      DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    primary key (mmsi, Begin_time, End_time),
    INDEX idx_avg_speed(avgSpeed, Event_categories)
)
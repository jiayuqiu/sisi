DROP TABLE IF EXISTS sisi.ShoreNet.tab_sailing_log;
CREATE TABLE sisi.ShoreNet.tab_sailing_log
(
    mmsi             bigint,
    Begin_time       bigint,
    End_time         bigint,
    Middle_lon       FLOAT(24),
    Middle_lat       FLOAT(24),
    Point_num        int,
    avgSpeed         FLOAT(24),
    nowPortName      VARCHAR(200),
    nowDockName      VARCHAR(200),
    nowBerthName     VARCHAR(200),
    province         VARCHAR(200),
    Event_categories VARCHAR(50),
    coal_dock_id     int,
    create_time      DATETIME NOT NULL DEFAULT (GETDATE()),
    update_time      DATETIME NOT NULL DEFAULT (GETDATE()),
    INDEX            idx_sailing_log (mmsi, Begin_time, End_time)
)
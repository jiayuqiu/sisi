DROP TABLE IF EXISTS ShoreNet.dim_cluster_version;
CREATE TABLE ShoreNet.dim_cluster_version
(
    Id               INT PRIMARY KEY IDENTITY,
    create_time      DATETIME NOT NULL DEFAULT (GETDATE()),
    update_time      DATETIME NOT NULL DEFAULT (GETDATE()),
)
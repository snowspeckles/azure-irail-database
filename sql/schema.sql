CREATE TABLE StationsRollback (
    station_id NVARCHAR(50) NOT NULL PRIMARY KEY,
    name NVARCHAR(200) NOT NULL,
    longitude FLOAT NULL,
    latitude FLOAT NULL,
    collected_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE Stations (
    station_id NVARCHAR(50) NOT NULL PRIMARY KEY,
    name NVARCHAR(200) NOT NULL,
    longitude FLOAT NULL,
    latitude FLOAT NULL,
    collected_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE TrainDepartures (
    departure_external_id NVARCHAR(50) NOT NULL,
    scheduled_time DATETIME2 NOT NULL,

    delay_seconds INT NOT NULL DEFAULT 0,
    is_cancelled BIT NOT NULL DEFAULT 0,
    
    origin_id NVARCHAR(50) NOT NULL,
    destination_id NVARCHAR(50) NULL, -- final destination (nullable initially)

    platform_planned NVARCHAR(10) NULL,
    platform_actual NVARCHAR(10) NULL,

    vehicle_id NVARCHAR(50) NOT NULL, -- required for enrichment
    collected_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_TrainDepartures
        PRIMARY KEY (departure_external_id, scheduled_time, origin_id),

    CONSTRAINT FK_Departures_Origin
        FOREIGN KEY (origin_id) REFERENCES Stations(station_id),

    CONSTRAINT FK_Departures_Destination
        FOREIGN KEY (destination_id) REFERENCES Stations(station_id)
);

-- CREATE TABLE Disturbances (
--     disturbance_id NVARCHAR(50) NOT NULL PRIMARY KEY,
--     title NVARCHAR(300) NOT NULL,
--     description NVARCHAR(MAX) NULL,
--     type NVARCHAR(20) NULL,
--     timestamp DATETIME2 NOT NULL,
--     collected_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
-- );

CREATE INDEX IX_Departures_Time
ON TrainDepartures (scheduled_time);

CREATE INDEX IX_Departures_Origin
ON TrainDepartures (origin_id);

CREATE INDEX IX_Departures_Delay
ON TrainDepartures (delay_seconds);

CREATE INDEX IX_Departures_Origin_Time
ON TrainDepartures (origin_id, scheduled_time);


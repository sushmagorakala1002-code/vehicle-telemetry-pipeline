/* =========================================
   Schema
   ========================================= */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

/* =========================================
   External Data Source & File Format
   (include ONLY if created in Synapse)
   ========================================= */
-- CREATE EXTERNAL DATA SOURCE gold_adls WITH ( LOCATION = 'abfss://gold@<storage>.dfs.core.windows.net' );
-- CREATE EXTERNAL FILE FORMAT delta_format WITH ( FORMAT_TYPE = DELTA );

/* =========================================
   External Tables (Gold)
   ========================================= */

IF OBJECT_ID('gold.vehicle_speed_5min', 'U') IS NOT NULL
BEGIN
    DROP EXTERNAL TABLE gold.vehicle_speed_5min;
END
GO
CREATE EXTERNAL TABLE gold.vehicle_speed_5min
 (
    vehicle_id VARCHAR(10),
    avg_speed FLOAT,
    window_start DATETIME2,
    window_end DATETIME2
)
WITH (
    LOCATION = 'vehicle_speed_5min',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = delta_format
);


IF OBJECT_ID('gold.vehicle_engine_temp_5min', 'U') IS NOT NULL
BEGIN
    DROP EXTERNAL TABLE gold.vehicle_engine_temp_5min;
END
GO
CREATE EXTERNAL TABLE gold.vehicle_engine_temp_5min
 (
    vehicle_id VARCHAR(10),
    max_engine_temp FLOAT,
    window_start DATETIME2,
    window_end DATETIME2
)
WITH (
    LOCATION = 'vehicle_engine_temp_5min',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = delta_format
);


IF OBJECT_ID('gold.vehicle_event_count_5min', 'U') IS NOT NULL
BEGIN
    DROP EXTERNAL TABLE gold.vehicle_event_count_5min;
END
GO
CREATE EXTERNAL TABLE gold.vehicle_event_count_5min
 (
    vehicle_id VARCHAR(10),
    event_count FLOAT,
    window_start DATETIME2,
    window_end DATETIME2
)
WITH (
    LOCATION = 'vehicle_event_count_5min',
    DATA_SOURCE = gold_adls,
    FILE_FORMAT = delta_format
);

/* =========================================
   Views (Power BI consumes THESE)
   ========================================= */

IF OBJECT_ID('gold.v_vehicle_speed_5min', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.v_vehicle_speed_5min;
END
GO
CREATE VIEW gold.v_vehicle_speed_5min AS
SELECT
    vehicle_id,
    avg_speed,
    window_start,
    window_end,
    DATEDIFF(minute, window_start, window_end) AS window_minutes
FROM gold.vehicle_speed_5min;


IF OBJECT_ID('gold.v_vehicle_eng_temp', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.v_vehicle_eng_temp;
END
GO
CREATE VIEW gold.v_vehicle_eng_temp AS
SELECT
    vehicle_id,
    max_engine_temp,
    window_start,
    window_end,
    DATEDIFF(minute, window_start, window_end) AS window_minutes
FROM gold.vehicle_engine_temp_5min;


IF OBJECT_ID('gold.vehicle_event_count_5min', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.vehicle_event_count_5min;
END
GO
CREATE VIEW gold.v_vehicle_event_count_5min AS
SELECT
    vehicle_id,
    event_count,
    window_start,
    window_end,
    DATEDIFF(minute, window_start, window_end) AS window_minutes
FROM gold.vehicle_event_count_5min;
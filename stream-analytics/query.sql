-- Stream Analytics query for CosmosDB output
SELECT
-- grouping by Location and 5 minute tumbling window
    location,
    System.TimeStamp() AS EventTime,

-- aggregations 
	AVG(ice_thickness) AS AvgIceThickness,
    AVG(surface_temperature) AS AvgSurfaceTemperature,
    AVG(external_temperature) AS AvgExternalTemperature,
    MIN(ice_thickness) AS MinIceThickness,
    MAX(ice_thickness) AS MaxIceThickness,
    MIN(surface_temperature) AS MinSurfaceTemperature,
    MAX(surface_temperature) AS MaxSurfaceTemperature,
    MAX(snow_accumulation) AS MaxSnowAccumulation,
    count(*) AS ReadingCount,

-- safety status logic
    CASE
        WHEN AVG(ice_thickness) >= 30 AND AVG(surface_temperature) <= -2 THEN 'Safe'
        WHEN AVG(ice_thickness) >= 25 AND AVG(surface_temperature) <= 0 THEN 'Caution'
        Else 'Unsafe'
    END AS SafetyStatus

INTO
	[SensorAggregations]
FROM
	[Rideau-IoT-Ingest] 
GROUP BY 
    location,
    TumblingWindow(minute, 5)

-- Stream Analytics query for Blob Storage Historical Data output

SELECT
-- grouping by Location and 5 minute tumbling window
    location,
    System.TimeStamp() AS EventTime,

-- aggregations 
	AVG(ice_thickness) AS AvgIceThickness,
    AVG(surface_temperature) AS AvgSurfaceTemperature,
    AVG(external_temperature) AS AvgExternalTemperature,
    MIN(ice_thickness) AS MinIceThickness,
    MAX(ice_thickness) AS MaxIceThickness,
    MIN(surface_temperature) AS MinSurfaceTemperature,
    MAX(surface_temperature) AS MaxSurfaceTemperature,
    MAX(snow_accumulation) AS MaxSnowAccumulation,
    count(*) AS ReadingCount,

-- safety status logic
    CASE
        WHEN AVG(ice_thickness) >= 30 AND AVG(surface_temperature) <= -2 THEN 'Safe'
        WHEN AVG(ice_thickness) >= 25 AND AVG(surface_temperature) <= 0 THEN 'Caution'
        Else 'Unsafe'
    END AS SafetyStatus

INTO
	[historical-sensor-aggregations]

FROM
	[Rideau-IoT-Ingest] 
GROUP BY 
    location,
    TumblingWindow(minute, 5)


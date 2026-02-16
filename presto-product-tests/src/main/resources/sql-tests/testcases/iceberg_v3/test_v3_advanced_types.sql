-- database: presto; groups: iceberg_v3, iceberg;
-- Test cases for advanced and future data types with Iceberg v3 tables
-- This includes: GEOMETRY types, UNKNOWN type, and Timestamp with nanosecond precision

-- ============================================================================
-- TEST: UNKNOWN type with v3 tables
-- ============================================================================
-- The UNKNOWN type in Presto represents values that are always NULL

CREATE TABLE iceberg.tpch.test_v3_unknown_type (
    id INTEGER,
    name VARCHAR,
    unknown_col UNKNOWN
) WITH (
    format_version = '3'
);

-- Insert data with UNKNOWN column (always NULL)
INSERT INTO iceberg.tpch.test_v3_unknown_type VALUES (1, 'test1', NULL);
INSERT INTO iceberg.tpch.test_v3_unknown_type VALUES (2, 'test2', NULL);

-- Query the data
SELECT id, name, unknown_col FROM iceberg.tpch.test_v3_unknown_type ORDER BY id;

-- Verify UNKNOWN is always NULL
SELECT id, name, 
       CASE WHEN unknown_col IS NULL THEN 'NULL' ELSE 'NOT NULL' END as unknown_status
FROM iceberg.tpch.test_v3_unknown_type 
ORDER BY id;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_unknown_type;

-- ============================================================================
-- TEST: GEOMETRY type with v3 tables (if supported)
-- ============================================================================
-- Note: Geometry types are part of the geospatial plugin and may require 
-- special configuration to use with Iceberg tables. This test documents
-- the expected behavior.

-- Attempt to create v3 table with GEOMETRY type
-- This may fail if Geometry is not supported in Iceberg serialization

-- CREATE TABLE iceberg.tpch.test_v3_geometry_type (
--     id INTEGER,
--     location VARCHAR,
--     point_geom GEOMETRY
-- ) WITH (
--     format_version = '3'
-- );

-- If the above creation fails, it indicates that GEOMETRY types are not 
-- yet supported in Presto's Iceberg connector. Expected error:
-- "Cannot convert from Presto type 'GEOMETRY' to Iceberg type"

-- Alternative: Use VARCHAR/VARBINARY to store geometry as WKT/WKB
CREATE TABLE iceberg.tpch.test_v3_geometry_workaround (
    id INTEGER,
    location VARCHAR,
    point_wkt VARCHAR,  -- Well-Known Text representation
    point_wkb VARBINARY -- Well-Known Binary representation
) WITH (
    format_version = '3'
);

-- Insert geometry data as text/binary
INSERT INTO iceberg.tpch.test_v3_geometry_workaround VALUES 
    (1, 'New York', 'POINT(-74.006 40.7128)', X'0101000000000000000000C052C00000000000004440'),
    (2, 'London', 'POINT(-0.1276 51.5074)', X'0101000000713D0AD7A370B0BF1F85EB51B81E4940');

-- Query the geometry data
SELECT id, location, point_wkt FROM iceberg.tpch.test_v3_geometry_workaround ORDER BY id;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_geometry_workaround;

-- ============================================================================
-- TEST: Timestamp with nanosecond precision (Iceberg v3 feature)
-- ============================================================================
-- Note: Iceberg v3 introduces timestamp_ns (nanosecond precision) but this
-- is not yet exposed as a separate type in Presto. Presto's TIMESTAMP type
-- uses microsecond precision.

-- Current behavior: TIMESTAMP in Presto has microsecond precision
CREATE TABLE iceberg.tpch.test_v3_timestamp_precision (
    id INTEGER,
    event_name VARCHAR,
    event_timestamp TIMESTAMP,
    event_timestamp_tz TIMESTAMP WITH TIME ZONE
) WITH (
    format_version = '3'
);

-- Insert timestamp data (microsecond precision)
INSERT INTO iceberg.tpch.test_v3_timestamp_precision VALUES 
    (1, 'Event A', TIMESTAMP '2024-01-15 10:30:45.123456', TIMESTAMP '2024-01-15 10:30:45.123456 UTC'),
    (2, 'Event B', TIMESTAMP '2024-01-15 10:30:45.789012', TIMESTAMP '2024-01-15 10:30:45.789012 UTC');

-- Query timestamp data
SELECT id, event_name, event_timestamp, event_timestamp_tz 
FROM iceberg.tpch.test_v3_timestamp_precision 
ORDER BY id;

-- Test timestamp precision functions
SELECT id,
       event_timestamp,
       date_trunc('second', event_timestamp) as truncated_second,
       date_trunc('millisecond', event_timestamp) as truncated_millisecond
FROM iceberg.tpch.test_v3_timestamp_precision
ORDER BY id;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_timestamp_precision;

-- ============================================================================
-- TEST: VARIANT type (Future feature - not yet in Presto)
-- ============================================================================
-- Note: VARIANT is a flexible schema-less type proposed for future versions
-- of Presto and Iceberg. It is not currently supported.

-- The following would be the expected syntax if VARIANT were supported:
-- CREATE TABLE iceberg.tpch.test_v3_variant_type (
--     id INTEGER,
--     json_data VARIANT
-- ) WITH (
--     format_version = '3'
-- );

-- Workaround: Use VARCHAR/JSON type for flexible schema data
CREATE TABLE iceberg.tpch.test_v3_json_workaround (
    id INTEGER,
    name VARCHAR,
    metadata VARCHAR  -- Store JSON as string
) WITH (
    format_version = '3'
);

-- Insert JSON-like data as strings
INSERT INTO iceberg.tpch.test_v3_json_workaround VALUES 
    (1, 'User1', '{"age": 25, "city": "NYC"}'),
    (2, 'User2', '{"age": 30, "city": "LA", "active": true}');

-- Query and parse JSON strings (using Presto JSON functions)
SELECT id, name, 
       json_extract_scalar(metadata, '$.age') as age,
       json_extract_scalar(metadata, '$.city') as city
FROM iceberg.tpch.test_v3_json_workaround
ORDER BY id;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_json_workaround;

-- ============================================================================
-- Summary of Advanced Type Support in Iceberg v3
-- ============================================================================
-- 
-- Type                          | Status in Presto Iceberg v3
-- ------------------------------|--------------------------------
-- UNKNOWN                       | Supported (always NULL)
-- GEOMETRY                      | Not directly supported in Iceberg
--                              | Workaround: Use VARCHAR (WKT) or VARBINARY (WKB)
-- Timestamp (microsecond)       | Fully supported
-- Timestamp (nanosecond)        | Not yet exposed in Presto
--                              | Iceberg v3 has timestamp_ns but Presto uses microsecond
-- VARIANT                       | Not yet implemented
--                              | Workaround: Use VARCHAR with JSON functions
--
-- ============================================================================

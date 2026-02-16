-- database: presto; groups: iceberg_v3, iceberg;
-- Test that all Presto data types work correctly with Iceberg v3 tables
-- This test verifies comprehensive data type support in format version 3

-- ============================================================================
-- TEST: All basic data types in v3 tables
-- ============================================================================

-- Create a v3 table with all supported data types
CREATE TABLE iceberg.tpch.test_v3_all_types (
    -- Numeric types
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_real REAL,
    col_double DOUBLE,
    col_decimal_small DECIMAL(10, 2),
    col_decimal_large DECIMAL(38, 10),
    
    -- String types
    col_varchar VARCHAR,
    col_varchar_bounded VARCHAR(100),
    col_char CHAR(10),
    
    -- Binary type
    col_varbinary VARBINARY,
    
    -- Date and time types
    col_date DATE,
    col_time TIME,
    col_timestamp TIMESTAMP,
    col_timestamp_tz TIMESTAMP WITH TIME ZONE,
    
    -- UUID type
    col_uuid UUID,
    
    -- Complex types
    col_array ARRAY(INTEGER),
    col_map MAP(VARCHAR, INTEGER),
    col_row ROW(field1 INTEGER, field2 VARCHAR, field3 DOUBLE)
) WITH (
    format_version = '3'
);

-- Insert test data covering all types
INSERT INTO iceberg.tpch.test_v3_all_types VALUES (
    -- Numeric types
    true,
    TINYINT '127',
    SMALLINT '32767',
    42,
    9223372036854775807,
    REAL '3.14',
    2.718281828,
    DECIMAL '12345.67',
    DECIMAL '12345678901234567890.1234567890',
    
    -- String types
    'varchar value',
    'bounded varchar',
    'char value',
    
    -- Binary type
    X'DEADBEEF',
    
    -- Date and time types
    DATE '2024-01-15',
    TIME '14:30:00',
    TIMESTAMP '2024-01-15 14:30:00',
    TIMESTAMP '2024-01-15 14:30:00 UTC',
    
    -- UUID type
    UUID '550e8400-e29b-41d4-a716-446655440000',
    
    -- Complex types
    ARRAY[1, 2, 3, 4, 5],
    MAP(ARRAY['key1', 'key2'], ARRAY[100, 200]),
    ROW(123, 'nested value', 45.67)
);

-- Insert NULL values for all types
INSERT INTO iceberg.tpch.test_v3_all_types VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL
);

-- Verify data types and query results
SELECT col_boolean, col_integer, col_bigint, col_double 
FROM iceberg.tpch.test_v3_all_types 
WHERE col_boolean IS NOT NULL
ORDER BY col_integer;

SELECT col_varchar, col_date, col_timestamp 
FROM iceberg.tpch.test_v3_all_types 
WHERE col_varchar IS NOT NULL;

SELECT col_array, col_map 
FROM iceberg.tpch.test_v3_all_types 
WHERE col_array IS NOT NULL;

SELECT col_row.field1, col_row.field2, col_row.field3 
FROM iceberg.tpch.test_v3_all_types 
WHERE col_row IS NOT NULL;

-- Test decimal precision
SELECT col_decimal_small, col_decimal_large 
FROM iceberg.tpch.test_v3_all_types 
WHERE col_decimal_small IS NOT NULL;

-- Test UUID operations
SELECT col_uuid, CAST(col_uuid AS VARCHAR) as uuid_string
FROM iceberg.tpch.test_v3_all_types 
WHERE col_uuid IS NOT NULL;

-- Test array operations
SELECT cardinality(col_array) as array_size, 
       array_sum(col_array) as array_total
FROM iceberg.tpch.test_v3_all_types 
WHERE col_array IS NOT NULL;

-- Test map operations
SELECT map_keys(col_map) as keys, 
       map_values(col_map) as values
FROM iceberg.tpch.test_v3_all_types 
WHERE col_map IS NOT NULL;

-- Count total rows
SELECT count(*) as total_rows FROM iceberg.tpch.test_v3_all_types;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_all_types;

-- ============================================================================
-- TEST: Complex nested types in v3 tables
-- ============================================================================

CREATE TABLE iceberg.tpch.test_v3_nested_types (
    id INTEGER,
    nested_array ARRAY(ARRAY(INTEGER)),
    nested_map MAP(VARCHAR, MAP(VARCHAR, INTEGER)),
    nested_row ROW(
        simple_field VARCHAR,
        array_field ARRAY(INTEGER),
        map_field MAP(VARCHAR, DOUBLE),
        row_field ROW(x INTEGER, y INTEGER)
    )
) WITH (
    format_version = '3'
);

-- Insert nested data
INSERT INTO iceberg.tpch.test_v3_nested_types VALUES (
    1,
    ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, 6]],
    MAP(ARRAY['outer1', 'outer2'], ARRAY[
        MAP(ARRAY['inner1'], ARRAY[10]),
        MAP(ARRAY['inner2'], ARRAY[20])
    ]),
    ROW(
        'test',
        ARRAY[100, 200, 300],
        MAP(ARRAY['a', 'b'], ARRAY[1.1, 2.2]),
        ROW(10, 20)
    )
);

-- Query nested structures
SELECT id, nested_array FROM iceberg.tpch.test_v3_nested_types;

SELECT id, nested_row.simple_field, nested_row.array_field 
FROM iceberg.tpch.test_v3_nested_types;

SELECT id, nested_row.row_field.x, nested_row.row_field.y 
FROM iceberg.tpch.test_v3_nested_types;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_nested_types;

-- ============================================================================
-- TEST: Partitioned v3 table with various data types
-- ============================================================================

CREATE TABLE iceberg.tpch.test_v3_partitioned_types (
    id INTEGER,
    name VARCHAR,
    amount DECIMAL(10, 2),
    category VARCHAR,
    created_date DATE,
    tags ARRAY(VARCHAR),
    metadata MAP(VARCHAR, VARCHAR)
) WITH (
    format_version = '3',
    partitioning = ARRAY['category', 'bucket(id, 4)']
);

-- Insert data across multiple partitions
INSERT INTO iceberg.tpch.test_v3_partitioned_types VALUES
    (1, 'Item A', 100.50, 'Electronics', DATE '2024-01-01', ARRAY['new', 'featured'], MAP(ARRAY['color'], ARRAY['blue'])),
    (2, 'Item B', 250.75, 'Electronics', DATE '2024-01-02', ARRAY['sale'], MAP(ARRAY['color'], ARRAY['red'])),
    (3, 'Item C', 75.00, 'Books', DATE '2024-01-01', ARRAY['bestseller'], MAP(ARRAY['genre'], ARRAY['fiction'])),
    (4, 'Item D', 150.25, 'Books', DATE '2024-01-03', ARRAY['new'], MAP(ARRAY['genre'], ARRAY['non-fiction'])),
    (5, 'Item E', 300.00, 'Electronics', DATE '2024-01-01', ARRAY['premium'], MAP(ARRAY['color'], ARRAY['black']));

-- Query partitioned data
SELECT category, count(*), sum(amount) 
FROM iceberg.tpch.test_v3_partitioned_types 
GROUP BY category 
ORDER BY category;

-- Query specific partition
SELECT * FROM iceberg.tpch.test_v3_partitioned_types 
WHERE category = 'Electronics' 
ORDER BY id;

-- Query with complex type filters
SELECT id, name, tags 
FROM iceberg.tpch.test_v3_partitioned_types 
WHERE contains(tags, 'new')
ORDER BY id;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_partitioned_types;

-- ============================================================================
-- TEST: Edge cases and special values
-- ============================================================================

CREATE TABLE iceberg.tpch.test_v3_edge_cases (
    id INTEGER,
    empty_varchar VARCHAR,
    empty_array ARRAY(INTEGER),
    empty_map MAP(VARCHAR, INTEGER),
    max_bigint BIGINT,
    min_bigint BIGINT,
    special_double DOUBLE,
    negative_decimal DECIMAL(10, 2)
) WITH (
    format_version = '3'
);

-- Insert edge case values
INSERT INTO iceberg.tpch.test_v3_edge_cases VALUES
    (1, '', ARRAY[], MAP(ARRAY[], ARRAY[]), 9223372036854775807, -9223372036854775808, DOUBLE 'Infinity', DECIMAL '-9999.99'),
    (2, 'normal', ARRAY[0], MAP(ARRAY['zero'], ARRAY[0]), 0, 0, 0.0, DECIMAL '0.00'),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Query edge cases
SELECT * FROM iceberg.tpch.test_v3_edge_cases ORDER BY id;

-- Test empty collections
SELECT id, cardinality(empty_array), cardinality(map_keys(empty_map))
FROM iceberg.tpch.test_v3_edge_cases
WHERE empty_array IS NOT NULL;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_edge_cases;

-- ============================================================================
-- End of data types tests
-- ============================================================================

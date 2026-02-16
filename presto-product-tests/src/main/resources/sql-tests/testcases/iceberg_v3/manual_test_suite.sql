-- ============================================================================
-- Iceberg Table v3 Manual Test Suite
-- ============================================================================
-- This file contains comprehensive manual tests for verifying that unsupported
-- Iceberg table v3 features properly return error messages in Presto.
--
-- Each test section includes:
-- - Setup (CREATE TABLE, INSERT data)
-- - Test operation (that should fail)
-- - Expected error message
-- - Cleanup (DROP TABLE)
--
-- Run each section independently and verify the expected error is returned.
-- ============================================================================

-- ============================================================================
-- TEST 1: DELETE operations on v3 tables
-- ============================================================================
-- Expected Error: "Iceberg table updates for format version 3 are not supported yet"

CREATE TABLE iceberg.tpch.test_v3_delete (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
) WITH (
    format_version = '3',
    "write.delete.mode" = 'merge-on-read'
);

INSERT INTO iceberg.tpch.test_v3_delete VALUES 
    (1, 'Alice', 100.0),
    (2, 'Bob', 200.0),
    (3, 'Charlie', 300.0);

-- Verify insert succeeded
SELECT * FROM iceberg.tpch.test_v3_delete ORDER BY id;

-- This DELETE should FAIL with error about v3 not supported
DELETE FROM iceberg.tpch.test_v3_delete WHERE id = 1;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_delete;

-- ============================================================================
-- TEST 2: UPDATE operations on v3 tables
-- ============================================================================
-- Expected Error: "Iceberg table updates for format version 3 are not supported yet"

CREATE TABLE iceberg.tpch.test_v3_update (
    id INTEGER,
    name VARCHAR,
    status VARCHAR,
    score DOUBLE
) WITH (
    format_version = '3',
    "write.update.mode" = 'merge-on-read'
);

INSERT INTO iceberg.tpch.test_v3_update VALUES 
    (1, 'Alice', 'active', 85.5),
    (2, 'Bob', 'active', 92.0),
    (3, 'Charlie', 'inactive', 78.3);

-- Verify insert succeeded
SELECT * FROM iceberg.tpch.test_v3_update ORDER BY id;

-- This UPDATE should FAIL with error about v3 not supported
UPDATE iceberg.tpch.test_v3_update 
SET status = 'updated', score = 95.0 
WHERE id = 1;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_update;

-- ============================================================================
-- TEST 3: MERGE operations on v3 tables
-- ============================================================================
-- Expected Error: "Iceberg table updates for format version 3 are not supported yet"

CREATE TABLE iceberg.tpch.test_v3_merge_target (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
) WITH (
    format_version = '3',
    "write.update.mode" = 'merge-on-read'
);

CREATE TABLE iceberg.tpch.test_v3_merge_source (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
);

INSERT INTO iceberg.tpch.test_v3_merge_target VALUES 
    (1, 'Alice', 100.0),
    (2, 'Bob', 200.0);

INSERT INTO iceberg.tpch.test_v3_merge_source VALUES 
    (1, 'Alice Updated', 150.0),
    (3, 'Charlie', 300.0);

-- Verify inserts succeeded
SELECT * FROM iceberg.tpch.test_v3_merge_target ORDER BY id;
SELECT * FROM iceberg.tpch.test_v3_merge_source ORDER BY id;

-- This MERGE should FAIL with error about v3 not supported
MERGE INTO iceberg.tpch.test_v3_merge_target t 
USING iceberg.tpch.test_v3_merge_source s 
ON t.id = s.id 
WHEN MATCHED THEN 
    UPDATE SET name = s.name, value = s.value
WHEN NOT MATCHED THEN 
    INSERT (id, name, value) VALUES (s.id, s.name, s.value);

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_merge_target;
DROP TABLE iceberg.tpch.test_v3_merge_source;

-- ============================================================================
-- TEST 4: OPTIMIZE (rewrite_data_files) operations on v3 tables
-- ============================================================================
-- Expected Error: "OPTIMIZE is not supported for Iceberg table format version > 2"

CREATE TABLE iceberg.tpch.test_v3_optimize (
    id INTEGER,
    category VARCHAR,
    value DOUBLE
) WITH (
    format_version = '3'
);

-- Insert multiple times to create small files
INSERT INTO iceberg.tpch.test_v3_optimize VALUES (1, 'A', 100.0);
INSERT INTO iceberg.tpch.test_v3_optimize VALUES (2, 'B', 200.0);
INSERT INTO iceberg.tpch.test_v3_optimize VALUES (3, 'A', 150.0);
INSERT INTO iceberg.tpch.test_v3_optimize VALUES (4, 'C', 300.0);

-- Verify inserts succeeded
SELECT * FROM iceberg.tpch.test_v3_optimize ORDER BY id;

-- This OPTIMIZE should FAIL with error about v3 not supported
CALL iceberg.system.rewrite_data_files('tpch', 'test_v3_optimize');

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_optimize;

-- ============================================================================
-- TEST 5: Verify v3 tables support basic operations
-- ============================================================================
-- This test verifies that v3 tables DO work for supported operations

CREATE TABLE iceberg.tpch.test_v3_supported (
    id INTEGER,
    name VARCHAR,
    created_date DATE,
    amount DECIMAL(10,2)
) WITH (
    format_version = '3',
    partitioning = ARRAY['created_date']
);

-- INSERT should work
INSERT INTO iceberg.tpch.test_v3_supported VALUES 
    (1, 'Transaction A', DATE '2024-01-01', 100.50),
    (2, 'Transaction B', DATE '2024-01-02', 250.75),
    (3, 'Transaction C', DATE '2024-01-01', 175.00);

-- SELECT should work
SELECT * FROM iceberg.tpch.test_v3_supported ORDER BY id;

-- Aggregations should work
SELECT created_date, count(*), sum(amount) 
FROM iceberg.tpch.test_v3_supported 
GROUP BY created_date 
ORDER BY created_date;

-- Partitioned queries should work
SELECT * FROM iceberg.tpch.test_v3_supported 
WHERE created_date = DATE '2024-01-01' 
ORDER BY id;

-- More INSERT should work
INSERT INTO iceberg.tpch.test_v3_supported VALUES 
    (4, 'Transaction D', DATE '2024-01-03', 300.00);

SELECT count(*) as total_count FROM iceberg.tpch.test_v3_supported;

-- Cleanup
DROP TABLE iceberg.tpch.test_v3_supported;

-- ============================================================================
-- End of Test Suite
-- ============================================================================
-- Summary of Expected Behaviors:
-- 1. DELETE on v3 tables: FAILS with "format version 3 are not supported yet"
-- 2. UPDATE on v3 tables: FAILS with "format version 3 are not supported yet"
-- 3. MERGE on v3 tables: FAILS with "format version 3 are not supported yet"
-- 4. OPTIMIZE on v3 tables: FAILS with "format version > 2" not supported
-- 5. CREATE, INSERT, SELECT on v3 tables: WORKS successfully
-- 
-- Note: For comprehensive data type testing with v3 tables, see test_v3_data_types.sql
-- which validates all Presto data types (primitive, string, temporal, UUID, and 
-- complex types like ARRAY, MAP, ROW) work correctly with format version 3.
-- ============================================================================

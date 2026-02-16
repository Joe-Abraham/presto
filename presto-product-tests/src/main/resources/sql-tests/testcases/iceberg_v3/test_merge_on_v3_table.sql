-- database: presto; groups: iceberg_v3, iceberg;
-- Test that MERGE operations on Iceberg v3 tables return proper error messages
-- Expected error: "Iceberg table updates for format version 3 are not supported yet"

-- Create a v3 target table with merge-on-read update mode
CREATE TABLE iceberg.tpch.test_v3_merge_target (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
) WITH (
    format_version = '3',
    "write.update.mode" = 'merge-on-read'
);

-- Create a source table for merge operation
CREATE TABLE iceberg.tpch.test_v3_merge_source (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
);

-- Insert test data into both tables
INSERT INTO iceberg.tpch.test_v3_merge_target VALUES 
    (1, 'Alice', 100.0),
    (2, 'Bob', 200.0);

INSERT INTO iceberg.tpch.test_v3_merge_source VALUES 
    (1, 'Alice Updated', 150.0),
    (3, 'Charlie', 300.0);

-- Verify data was inserted successfully
SELECT 'Target Table' as source, * FROM iceberg.tpch.test_v3_merge_target ORDER BY id;
SELECT 'Source Table' as source, * FROM iceberg.tpch.test_v3_merge_source ORDER BY id;

-- Attempt MERGE operation - should fail with error message
-- Expected error: "Iceberg table updates for format version 3 are not supported yet"
MERGE INTO iceberg.tpch.test_v3_merge_target t 
USING iceberg.tpch.test_v3_merge_source s 
ON t.id = s.id 
WHEN MATCHED THEN 
    UPDATE SET name = s.name, value = s.value
WHEN NOT MATCHED THEN 
    INSERT (id, name, value) VALUES (s.id, s.name, s.value);

-- Cleanup
DROP TABLE IF EXISTS iceberg.tpch.test_v3_merge_target;
DROP TABLE IF EXISTS iceberg.tpch.test_v3_merge_source;

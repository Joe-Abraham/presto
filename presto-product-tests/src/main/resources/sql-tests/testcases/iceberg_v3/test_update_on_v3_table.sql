-- database: presto; groups: iceberg_v3, iceberg;
-- Test that UPDATE operations on Iceberg v3 tables return proper error messages
-- Expected error: "Iceberg table updates for format version 3 are not supported yet"

-- Create a v3 table with merge-on-read update mode
CREATE TABLE iceberg.tpch.test_v3_update_table (
    id INTEGER,
    name VARCHAR,
    status VARCHAR,
    score DOUBLE
) WITH (
    "format-version" = '3',
    "write.update.mode" = 'merge-on-read'
);

-- Insert some test data
INSERT INTO iceberg.tpch.test_v3_update_table VALUES 
    (1, 'Alice', 'active', 85.5),
    (2, 'Bob', 'active', 92.0),
    (3, 'Charlie', 'inactive', 78.3);

-- Verify data was inserted successfully
SELECT * FROM iceberg.tpch.test_v3_update_table ORDER BY id;

-- Attempt UPDATE operation - should fail with error message
-- Expected error: "Iceberg table updates for format version 3 are not supported yet"
UPDATE iceberg.tpch.test_v3_update_table 
SET status = 'updated', score = 95.0 
WHERE id = 1;

-- Cleanup
DROP TABLE IF EXISTS iceberg.tpch.test_v3_update_table;

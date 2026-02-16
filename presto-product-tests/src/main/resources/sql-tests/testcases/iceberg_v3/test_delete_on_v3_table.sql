-- database: presto; groups: iceberg_v3, iceberg; 
-- Test that DELETE operations on Iceberg v3 tables return proper error messages
-- Expected error: "Iceberg table updates for format version 3 are not supported yet"

-- Create a v3 table with merge-on-read delete mode
CREATE TABLE iceberg.tpch.test_v3_delete_table (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
) WITH (
    format_version = '3',
    "write.delete.mode" = 'merge-on-read'
);

-- Insert some test data
INSERT INTO iceberg.tpch.test_v3_delete_table VALUES 
    (1, 'Alice', 100.0),
    (2, 'Bob', 200.0),
    (3, 'Charlie', 300.0);

-- Verify data was inserted successfully
SELECT * FROM iceberg.tpch.test_v3_delete_table ORDER BY id;

-- Attempt DELETE operation - should fail with error message
-- Expected error: "Iceberg table updates for format version 3 are not supported yet"
DELETE FROM iceberg.tpch.test_v3_delete_table WHERE id = 1;

-- Cleanup
DROP TABLE IF EXISTS iceberg.tpch.test_v3_delete_table;

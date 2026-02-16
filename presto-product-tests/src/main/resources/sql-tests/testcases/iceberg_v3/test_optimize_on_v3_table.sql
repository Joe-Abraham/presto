-- database: presto; groups: iceberg_v3, iceberg;
-- Test that OPTIMIZE (rewrite_data_files) operations on Iceberg v3 tables return proper error messages
-- Expected error: "OPTIMIZE is not supported for Iceberg table format version > 2"

-- Create a v3 table
CREATE TABLE iceberg.tpch.test_v3_optimize_table (
    id INTEGER,
    category VARCHAR,
    value DOUBLE
) WITH (
    "format-version" = '3'
);

-- Insert multiple small files to create a scenario for optimization
INSERT INTO iceberg.tpch.test_v3_optimize_table VALUES (1, 'A', 100.0);
INSERT INTO iceberg.tpch.test_v3_optimize_table VALUES (2, 'B', 200.0);
INSERT INTO iceberg.tpch.test_v3_optimize_table VALUES (3, 'A', 150.0);
INSERT INTO iceberg.tpch.test_v3_optimize_table VALUES (4, 'C', 300.0);

-- Verify data was inserted successfully
SELECT * FROM iceberg.tpch.test_v3_optimize_table ORDER BY id;

-- Attempt OPTIMIZE operation using rewrite_data_files procedure - should fail with error message
-- Expected error: "OPTIMIZE is not supported for Iceberg table format version > 2"
CALL iceberg.system.rewrite_data_files('tpch', 'test_v3_optimize_table');

-- Cleanup
DROP TABLE IF EXISTS iceberg.tpch.test_v3_optimize_table;

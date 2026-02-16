-- database: presto; groups: iceberg_v3, iceberg;
-- Test that reading Iceberg v3 tables with deletion vectors (PUFFIN format) returns proper error messages
-- Expected error: "Iceberg deletion vectors (PUFFIN format) are not supported"

-- NOTE: This test requires a v3 table with PUFFIN deletion vectors to be created externally
-- using the Iceberg library, as deletion vectors are a v3-specific feature that Presto 
-- does not yet support reading.

-- The following is an example of how such a table would be queried:
-- Assuming a table 'test_v3_deletion_vectors' exists with PUFFIN deletion vectors

-- SELECT * FROM iceberg.tpch.test_v3_deletion_vectors;
-- Expected error: "Iceberg deletion vectors (PUFFIN format) are not supported"

-- This test case documents the expected behavior but cannot be executed 
-- directly through SQL without external table creation with deletion vectors.

/*
Example scenario to create a table with deletion vectors:

1. Create a v3 table using an external tool (Spark, PyIceberg, etc.)
2. Perform row-level deletes using a tool that supports deletion vectors
3. The deletes will be stored in PUFFIN format files
4. Attempting to read this table in Presto will fail with the error

Example Spark code:
```scala
spark.sql("""
  CREATE TABLE iceberg_catalog.tpch.test_v3_deletion_vectors (
    id INT, 
    name STRING, 
    value DOUBLE
  ) 
  USING iceberg
  TBLPROPERTIES (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.delete.isolation-level' = 'serializable'
  )
""")

spark.sql("INSERT INTO iceberg_catalog.tpch.test_v3_deletion_vectors VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)")

// This may create deletion vectors in some configurations
spark.sql("DELETE FROM iceberg_catalog.tpch.test_v3_deletion_vectors WHERE id = 1")
```
*/

-- Once the table with deletion vectors exists, attempting to query it in Presto should fail:
-- SELECT * FROM iceberg.tpch.test_v3_deletion_vectors;
-- SELECT count(*) FROM iceberg.tpch.test_v3_deletion_vectors;

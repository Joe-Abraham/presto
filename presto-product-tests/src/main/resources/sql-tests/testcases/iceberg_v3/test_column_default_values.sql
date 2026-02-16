-- database: presto; groups: iceberg_v3, iceberg;
-- Test that reading Iceberg v3 tables with column default values returns proper error messages
-- Expected error: "Iceberg v3 column default values are not supported"

-- NOTE: This test requires a v3 table to be created externally with column default values
-- using the Iceberg library directly, as Presto does not support creating tables with 
-- default values through SQL DDL.

-- The following is an example of how such a table would be queried:
-- Assuming a table 'test_v3_column_defaults' exists with column default values

-- SELECT * FROM iceberg.tpch.test_v3_column_defaults;
-- Expected error: "Iceberg v3 column default values are not supported"

-- This test case documents the expected behavior but cannot be executed 
-- directly through SQL without external table creation.

/* 
Example Python code to create such a table using PyIceberg:

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType

catalog = load_catalog("my_catalog")
schema = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "name", StringType(), required=False, 
                initial_default="default_name", 
                write_default="default_name")
)

catalog.create_table(
    "tpch.test_v3_column_defaults",
    schema=schema,
    properties={"format-version": "3"}
)
*/

-- Once the table exists externally, attempting to read or describe it should fail:
-- SHOW COLUMNS FROM iceberg.tpch.test_v3_column_defaults;
-- SELECT * FROM iceberg.tpch.test_v3_column_defaults;

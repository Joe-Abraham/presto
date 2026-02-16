-- database: presto; groups: iceberg_v3, iceberg;
-- Manual test case for updating Iceberg table metadata to have initial-default and write-default
-- Expected behavior: Presto should return an error when attempting to read tables with column default values
-- Expected error: "Iceberg v3 column default values are not supported"

-- ============================================================================
-- MANUAL TEST: Update Iceberg Table Metadata with Column Default Values
-- ============================================================================

-- This test demonstrates how to manually update an Iceberg table's metadata to include
-- initial-default and write-default values for columns, and verifies that Presto 
-- correctly detects and reports an error when attempting to read such tables.

-- PREREQUISITES:
-- 1. Access to the Iceberg table's metadata location (file system or object store)
-- 2. Iceberg Java API library or PyIceberg installed
-- 3. Presto with Iceberg connector configured

-- ============================================================================
-- STEP 1: Create a regular v3 table in Presto (without defaults)
-- ============================================================================

CREATE TABLE iceberg.tpch.test_metadata_update_defaults (
    id INTEGER,
    name VARCHAR,
    status VARCHAR,
    created_date DATE
) WITH (
    "format-version" = '3'
);

-- Insert some initial data
INSERT INTO iceberg.tpch.test_metadata_update_defaults VALUES 
    (1, 'Alice', 'active', DATE '2024-01-01'),
    (2, 'Bob', 'active', DATE '2024-01-02');

-- Verify table can be read normally
SELECT * FROM iceberg.tpch.test_metadata_update_defaults ORDER BY id;

-- ============================================================================
-- STEP 2: Update table metadata using Iceberg API (external to Presto)
-- ============================================================================

/*
The following code snippets show how to update the table metadata externally
using different Iceberg API approaches:

-- METHOD A: Using Iceberg Java API --

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.*;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.hadoop.HadoopCatalog;

// Load the catalog and table
HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation);
TableIdentifier tableId = TableIdentifier.of("tpch", "test_metadata_update_defaults");
Table table = catalog.loadTable(tableId);

// Get current metadata
BaseTable baseTable = (BaseTable) table;
TableOperations ops = baseTable.operations();
TableMetadata current = ops.current();

// Create a new schema with default values
Schema newSchema = new Schema(
    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get())
        .withInitialDefault("default_name")
        .withWriteDefault("default_name"),
    Types.NestedField.optional(3, "status", Types.StringType.get())
        .withInitialDefault("pending")
        .withWriteDefault("active"),
    Types.NestedField.optional(4, "created_date", Types.DateType.get())
);

// Update the table metadata with new schema
TableMetadata updated = current.updateSchema(newSchema, current.lastColumnId());
ops.commit(current, updated);

-- METHOD B: Using PyIceberg (Python) --

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, DateType

# Load the catalog
catalog = load_catalog("my_catalog", **{
    "type": "hadoop",
    "warehouse": "file:///path/to/warehouse"
})

# Load the table
table = catalog.load_table(("tpch", "test_metadata_update_defaults"))

# Create updated schema with default values
new_schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
    NestedField(
        field_id=2, 
        name="name", 
        field_type=StringType(), 
        required=False,
        initial_default="default_name",
        write_default="default_name"
    ),
    NestedField(
        field_id=3, 
        name="status", 
        field_type=StringType(), 
        required=False,
        initial_default="pending",
        write_default="active"
    ),
    NestedField(field_id=4, name="created_date", field_type=DateType(), required=False)
)

# Update the table with new schema
with table.update_schema() as update:
    update.set_schema(new_schema)

-- METHOD C: Direct Metadata File Edit (Advanced) --

You can also manually edit the metadata JSON file, but this is not recommended
for production use. The metadata file location can be found from the table's
metadata pointer file.

Example metadata.json structure with column defaults:

{
  "format-version": 3,
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "required": false, "type": "int"},
      {
        "id": 2, 
        "name": "name", 
        "required": false, 
        "type": "string",
        "initial-default": "default_name",
        "write-default": "default_name"
      },
      {
        "id": 3, 
        "name": "status", 
        "required": false, 
        "type": "string",
        "initial-default": "pending",
        "write-default": "active"
      },
      {"id": 4, "name": "created_date", "required": false, "type": "date"}
    ]
  }
}

*/

-- ============================================================================
-- STEP 3: Attempt to query the table in Presto (should fail)
-- ============================================================================

-- After updating the metadata externally, attempting to query the table should fail
-- with an error message indicating that column default values are not supported

-- This query should FAIL with error:
-- "Iceberg v3 column default values are not supported"
SELECT * FROM iceberg.tpch.test_metadata_update_defaults;

-- This should also FAIL:
SELECT id, name FROM iceberg.tpch.test_metadata_update_defaults;

-- Even DESCRIBE should FAIL:
DESCRIBE iceberg.tpch.test_metadata_update_defaults;

-- SHOW COLUMNS should also FAIL:
SHOW COLUMNS FROM iceberg.tpch.test_metadata_update_defaults;

-- ============================================================================
-- STEP 4: Understanding the error detection
-- ============================================================================

-- The error is detected in IcebergAbstractMetadata.validateTableForPresto()
-- which checks:
--   for (Types.NestedField field : schema.columns()) {
--       if (field.initialDefault() != null || field.writeDefault() != null) {
--           throw new PrestoException(NOT_SUPPORTED, 
--               "Iceberg v3 column default values are not supported");
--       }
--   }

-- ============================================================================
-- VERIFICATION STEPS
-- ============================================================================

-- 1. Before updating metadata:
--    ✓ Table should be readable
--    ✓ SELECT, DESCRIBE, SHOW COLUMNS should work

-- 2. After updating metadata with defaults:
--    ✗ Any query attempt should fail with the specific error message
--    ✗ DESCRIBE and SHOW COLUMNS should also fail
--    ✗ Error should clearly indicate column default values are not supported

-- 3. To restore functionality:
--    - Either remove the default values from metadata
--    - Or create a new table without defaults and migrate data

-- ============================================================================
-- CLEANUP AND RESTORATION
-- ============================================================================

-- Note: If the metadata has been updated with defaults, this cleanup may fail.
-- To restore table accessibility, you need to remove the default values from the schema.

-- Method 1: Remove defaults using Java API
/*
Table table = catalog.loadTable(tableId);
BaseTable baseTable = (BaseTable) table;
TableOperations ops = baseTable.operations();
TableMetadata current = ops.current();

// Create schema without default values
Schema cleanSchema = new Schema(
    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get()),
    Types.NestedField.optional(3, "status", Types.StringType.get()),
    Types.NestedField.optional(4, "created_date", Types.DateType.get())
);

TableMetadata updated = current.updateSchema(cleanSchema, current.lastColumnId());
ops.commit(current, updated);
*/

-- Method 2: Remove defaults using PyIceberg
/*
table = catalog.load_table(("tpch", "test_metadata_update_defaults"))

clean_schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="status", field_type=StringType(), required=False),
    NestedField(field_id=4, name="created_date", field_type=DateType(), required=False)
)

with table.update_schema() as update:
    update.set_schema(clean_schema)
*/

-- After removing defaults, the table should be accessible again
DROP TABLE IF EXISTS iceberg.tpch.test_metadata_update_defaults;

-- ============================================================================
-- ADDITIONAL NOTES
-- ============================================================================

-- Column Default Values in Iceberg v3:
--
-- 1. initial-default: The value used for existing rows when a new column is added
--    Example: Adding a 'status' column with initial-default="pending" means all
--             existing rows will have status='pending'
--
-- 2. write-default: The value used for new rows when the column is not specified
--    Example: If write-default="active" for 'status', then
--             INSERT INTO table (id, name) VALUES (1, 'Alice')
--             would automatically set status='active'
--
-- Why Presto doesn't support this yet:
-- - Complex interaction with Presto's SQL semantics
-- - Need to handle default value evaluation and type conversion
-- - Requires coordination between metadata and query execution
--
-- Future work:
-- - Implement support for reading tables with default values
-- - Support writing with default values
-- - Allow creating tables with default values through DDL
--
-- ============================================================================
-- END OF MANUAL TEST CASE
-- ============================================================================

-- database: presto; groups: iceberg_v3, iceberg;
-- Manual test case for updating Iceberg table metadata to have initial-default and write-default
-- Expected behavior: Presto should return an error when attempting to read tables with column default values
-- Expected error: "Iceberg v3 column default values are not supported"

-- IMPORTANT NOTE: This is a manual test case providing conceptual guidance.
-- The exact Iceberg API methods may vary depending on the Iceberg library version you are using.
-- Always consult the Iceberg documentation for your specific version for accurate API usage.

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
using different Iceberg API approaches. Note: The exact API may vary by Iceberg version.

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
// Note: The exact API for setting defaults depends on Iceberg version
// The following is conceptual pseudocode - actual implementation varies
Schema newSchema = new Schema(
    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
    // In Iceberg 1.4+, configure fields with defaults before adding to schema
    // Exact syntax may vary - this is illustrative
    Types.NestedField.optional(2, "name", Types.StringType.get())
        .withInitialDefault("default_name")
        .withWriteDefault("default_name"),
    Types.NestedField.optional(3, "status", Types.StringType.get())
        .withInitialDefault("pending")
        .withWriteDefault("active"),
    Types.NestedField.optional(4, "created_date", Types.DateType.get())
);

// Alternative approach - build fields list first:
// List<Types.NestedField> fields = Arrays.asList(
//     Types.NestedField.optional(1, "id", Types.IntegerType.get()),
//     configureFieldWithDefaults(2, "name", ...),
//     ...
// );
// Schema newSchema = new Schema(fields);

// Update the table metadata with new schema
// API may vary by version - consult Iceberg documentation for your version
TableMetadata updated = TableMetadata.buildFrom(current)
    .setCurrentSchema(newSchema)
    .build();
ops.commit(current, updated);

-- METHOD B: Using PyIceberg (Python) --

from pyiceberg.catalog import load_catalog
from pyiceberg.types import NestedField, IntegerType, StringType, DateType

# Load the catalog
catalog = load_catalog("my_catalog", **{
    "type": "hadoop",
    "warehouse": "file:///path/to/warehouse"
})

# Load the table
table = catalog.load_table(("tpch", "test_metadata_update_defaults"))

# Note: PyIceberg schema evolution API varies by version
# The following is conceptual - consult PyIceberg docs for your version
# Approach: Add columns with default values one at a time

# Example: Update existing column to have defaults (conceptual)
# with table.update_schema() as update:
#     # Remove old column
#     update.drop_column("name")
#     # Add back with defaults
#     update.add_column(
#         field=NestedField(
#             field_id=2,
#             name="name",
#             field_type=StringType(),
#             required=False,
#             initial_default="default_name",
#             write_default="default_name"
#         )
#     )

# Alternative: Direct schema replacement (if supported by version)
# This is version-dependent and may not be available
# Check PyIceberg documentation for schema.update() or similar methods

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

// Build updated metadata without defaults
TableMetadata updated = TableMetadata.buildFrom(current)
    .setCurrentSchema(cleanSchema)
    .build();
ops.commit(current, updated);
*/

-- Method 2: Remove defaults using PyIceberg (conceptual)
/*
# Note: Exact API depends on PyIceberg version
table = catalog.load_table(("tpch", "test_metadata_update_defaults"))

# Option: Drop and re-add columns without defaults
# with table.update_schema() as update:
#     update.drop_column("name")
#     update.drop_column("status")
#     update.add_column(field_id=2, name="name", field_type=StringType(), required=False)
#     update.add_column(field_id=3, name="status", field_type=StringType(), required=False)

# Consult PyIceberg documentation for schema evolution methods in your version
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

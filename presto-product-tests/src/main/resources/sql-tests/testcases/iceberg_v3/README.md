# Iceberg Table v3 SQL Test Cases

This directory contains SQL test cases for manually testing that unsupported Iceberg table v3 features properly return error messages when requested.

## Background

Iceberg table format version 3 introduces new features, but not all of them are currently supported in Presto. These test cases verify that appropriate error messages are returned when users attempt to use unsupported v3 features.

## Test Cases

### Main Test Suite

#### `manual_test_suite.sql` - Comprehensive Test Suite

A complete test suite containing all tests in a single file for easy execution. This is recommended as the primary test file for manual testing. It includes:
- DELETE operation tests
- UPDATE operation tests
- MERGE operation tests
- OPTIMIZE operation tests
- Positive tests for supported v3 operations (CREATE, INSERT, SELECT)

### Individual Test Files

#### 1. DELETE Operations on v3 Tables (`test_delete_on_v3_table.sql`)

**Purpose**: Verify that DELETE operations on Iceberg v3 tables return proper error messages.

**Expected Error**: `"Iceberg table updates for format version 3 are not supported yet"`

**Description**: This test creates a v3 table with `merge-on-read` delete mode, inserts data, and attempts a DELETE operation which should fail with an appropriate error message.

#### 2. UPDATE Operations on v3 Tables (`test_update_on_v3_table.sql`)

**Purpose**: Verify that UPDATE operations on Iceberg v3 tables return proper error messages.

**Expected Error**: `"Iceberg table updates for format version 3 are not supported yet"`

**Description**: This test creates a v3 table with `merge-on-read` update mode, inserts data, and attempts an UPDATE operation which should fail with an appropriate error message.

#### 3. MERGE Operations on v3 Tables (`test_merge_on_v3_table.sql`)

**Purpose**: Verify that MERGE operations on Iceberg v3 tables return proper error messages.

**Expected Error**: `"Iceberg table updates for format version 3 are not supported yet"`

**Description**: This test creates a v3 target table and a source table, inserts data into both, and attempts a MERGE operation which should fail with an appropriate error message.

#### 4. OPTIMIZE Operations on v3 Tables (`test_optimize_on_v3_table.sql`)

**Purpose**: Verify that OPTIMIZE (rewrite_data_files) operations on Iceberg v3 tables return proper error messages.

**Expected Error**: `"OPTIMIZE is not supported for Iceberg table format version > 2"`

**Description**: This test creates a v3 table, inserts multiple rows to create small files, and attempts to run the rewrite_data_files procedure which should fail with an appropriate error message.

#### 5. Column Default Values (`test_column_default_values.sql`)

**Purpose**: Document the expected error when reading v3 tables with column default values.

**Expected Error**: `"Iceberg v3 column default values are not supported"`

**Description**: This test documents the behavior when Presto encounters a v3 table created externally with column default values. Note: This requires external table creation as Presto does not support creating tables with default values.

#### 5b. Manual Column Defaults Update (`manual_test_column_defaults_update.sql`)

**Purpose**: Provide a comprehensive manual test case for updating Iceberg table metadata to include initial-default and write-default values.

**Expected Error**: `"Iceberg v3 column default values are not supported"`

**Description**: This comprehensive manual test case demonstrates:
- How to create a v3 table in Presto
- Multiple methods to update table metadata externally (Java API, PyIceberg, direct JSON edit)
- Steps to verify Presto correctly detects and rejects tables with column defaults
- Detailed explanations of initial-default vs write-default
- Code examples in Java and Python for metadata updates

This is a step-by-step guide for manual testing that includes:
1. Creating a test table in Presto
2. External metadata update procedures (3 different methods)
3. Verification that Presto rejects the table after metadata update
4. Understanding the error detection mechanism
5. Cleanup procedures

#### 6. Deletion Vectors (PUFFIN Format) (`test_deletion_vectors_puffin.sql`)

**Purpose**: Document the expected error when reading v3 tables with PUFFIN deletion vectors.

**Expected Error**: `"Iceberg deletion vectors (PUFFIN format) are not supported"`

**Description**: This test documents the behavior when Presto encounters a v3 table with deletion vectors stored in PUFFIN format. Note: This requires external table creation with deletion vectors.

#### 7. Data Types Support (`test_v3_data_types.sql`)

**Purpose**: Verify that all Presto data types work correctly with Iceberg v3 tables.

**Description**: This comprehensive test validates that v3 tables support all data types including:
- **Primitive types**: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DECIMAL
- **String types**: VARCHAR, CHAR, VARBINARY
- **Temporal types**: DATE, TIME, TIMESTAMP, TIMESTAMP WITH TIME ZONE
- **Special types**: UUID
- **Complex types**: ARRAY, MAP, ROW (including nested structures)

The test includes:
- Creating v3 tables with all data types
- Inserting and querying data for each type
- Testing NULL values
- Testing complex nested structures (nested arrays, maps, rows)
- Testing partitioned tables with various data types
- Testing edge cases (empty collections, max/min values, special floating point values)

#### 8. Advanced Types (`test_v3_advanced_types.sql`)

**Purpose**: Document behavior and workarounds for advanced/future types with v3 tables.

**Description**: This test documents the current state of support for:
- **UNKNOWN type**: Presto's special type for NULL values (supported)
- **GEOMETRY type**: Geospatial types (not directly supported in Iceberg; workaround: use VARCHAR/VARBINARY)
- **Timestamp with nanosecond precision**: Iceberg v3 feature (not yet exposed in Presto; currently uses microsecond precision)
- **VARIANT type**: Future flexible schema type (not yet implemented; workaround: use VARCHAR with JSON functions)

## How to Run These Tests

These SQL test cases are designed for manual execution and testing. 

### Quick Start - Run the Complete Test Suite

The easiest way to test all v3 error messages is to run the comprehensive test suite:

```bash
# Using Presto CLI
presto-cli --catalog iceberg --schema tpch -f manual_test_suite.sql

# Or run interactively
presto-cli --catalog iceberg --schema tpch
# Then paste each test section from manual_test_suite.sql
```

### Running Individual Tests

You can run individual test files to test specific features:

1. **Presto CLI**: Execute the SQL files directly using the Presto command-line interface
   ```bash
   presto-cli --catalog iceberg --schema tpch -f test_delete_on_v3_table.sql
   presto-cli --catalog iceberg --schema tpch -f test_update_on_v3_table.sql
   presto-cli --catalog iceberg --schema tpch -f test_merge_on_v3_table.sql
   presto-cli --catalog iceberg --schema tpch -f test_optimize_on_v3_table.sql
   presto-cli --catalog iceberg --schema tpch -f test_v3_data_types.sql
   presto-cli --catalog iceberg --schema tpch -f test_v3_advanced_types.sql
   ```

2. **Product Tests Framework**: These tests can be integrated into the Presto product tests framework using Tempto

3. **Manual Testing**: Copy and paste the SQL statements into any Presto SQL client (DBeaver, DataGrip, etc.)

### Expected Test Results

For each test, you should see:
1. ✓ Tables created successfully
2. ✓ Data inserted successfully  
3. ✓ SELECT queries return expected results
4. ✗ **Row-level operations (DELETE/UPDATE/MERGE) FAIL with error message**
5. ✗ **OPTIMIZE operations FAIL with error message**
6. ✓ Tables dropped successfully

### Verifying Test Success

A test is successful when:
- The unsupported operation (DELETE/UPDATE/MERGE/OPTIMIZE) **fails** with the expected error message
- The error message clearly indicates the feature is not supported for v3
- All setup and cleanup operations succeed

## Implementation Details

### Format Version Limitation

The current implementation limits row-level operations to format version 2 or below:
- Constant: `MAX_FORMAT_VERSION_FOR_ROW_LEVEL_OPERATIONS = 2`
- Location: `presto-iceberg/src/main/java/com/facebook/presto/iceberg/IcebergUtil.java`

### Error Checking Locations

The format version checks and error messages are implemented in:
- `IcebergAbstractMetadata.java`: 
  - `beginDelete()` method (line ~1375)
  - `beginUpdate()` method (line ~1638)
  - `beginMerge()` method (line ~882)
- `RewriteDataFilesProcedure.java`: OPTIMIZE operation check

## Related Iceberg v3 Features

While these tests focus on unsupported features, Iceberg v3 also includes features that are supported:
- Basic CREATE TABLE with `format_version = '3'`
- INSERT operations into v3 tables
- SELECT queries from v3 tables
- Partitioned v3 tables
- **All standard Presto data types** (primitive, string, temporal, complex types)

Other v3-specific features not currently supported:
- Column default values (initial and write defaults)
- Deletion vectors (PUFFIN format)

## References

- Iceberg Format v3 Specification: https://iceberg.apache.org/spec/#version-3
- Related Test File: `presto-iceberg/src/test/java/com/facebook/presto/iceberg/TestIcebergV3.java`

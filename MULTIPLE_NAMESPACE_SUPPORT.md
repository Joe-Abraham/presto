# Multiple Namespace Support for Native Sidecar Function Registry

## Overview

This implementation adds support for registering functions under different catalogs and schemas in the native sidecar function registry, allowing users to namespace their custom C++ functions separately from built-in functions.

## Problem Statement

Previously, the native sidecar function registry only supported a single namespace (`native.default`), which meant:

1. All functions appeared under the same catalog/schema
2. Users couldn't separate custom functions from built-in functions
3. `SHOW FUNCTIONS` would only display functions from the default namespace
4. No feature parity with Java function namespace managers

## Solution

### Configuration Changes

Added a new configuration property to support multiple namespace prefixes:

```properties
# Default namespace (existing)
presto.default-namespace=native.default

# Additional namespaces (new)
presto.additional-namespace-prefixes=custom.analytics,ml.models,data.lake
```

### Implementation Details

#### 1. Configuration System (`Configs.h` / `Configs.cpp`)

- Added `kPrestoAdditionalNamespacePrefixes` configuration property
- Implemented `prestoAdditionalNamespacePrefixes()` method to parse comma-separated prefixes
- Automatic prefix formatting (adds trailing dot if missing)

#### 2. Function Registration (`PrestoServer.cpp`)

- Modified `registerFunctions()` to register functions with multiple prefixes
- Functions are now registered for each namespace prefix
- Logging added to track additional namespace registrations

#### 3. Endpoint Support (Already Existed)

- `/v1/functions` - Returns functions for all namespaces
- `/v1/functions/{catalog}` - Returns functions filtered by catalog
- Catalog filtering logic in `FunctionMetadata.cpp` already supported this

## Usage

### Configuration Example

```properties
# config.properties
presto.default-namespace=native.default
presto.additional-namespace-prefixes=custom.analytics,ml.models,data.lake
presto.native-sidecar=true
```

### Multiple Namespace Managers

Each catalog can have its own namespace manager:

```java
// Coordinator configuration
function-namespace-managers.catalog1=native
function-namespace-managers.catalog2=native
function-namespace-managers.catalog3=native
```

### Function Discovery

Functions will now be available under multiple catalogs:

```sql
-- Show functions from specific catalogs
SHOW FUNCTIONS FROM native.default;
SHOW FUNCTIONS FROM custom.analytics;
SHOW FUNCTIONS FROM ml.models;
SHOW FUNCTIONS FROM data.lake;

-- Use functions with qualified names
SELECT native.default.abs(-5);
SELECT custom.analytics.correlation(x, y);
SELECT ml.models.predict(model, features);
SELECT data.lake.parse_json(json_string);
```

## Benefits

1. **Namespace Separation**: Custom functions can be organized under logical catalogs
2. **Feature Parity**: Matches functionality available with Java function namespace managers
3. **Backward Compatibility**: Existing configurations continue to work unchanged
4. **Flexible Organization**: Functions can be grouped by domain (analytics, ML, data lake, etc.)

## Testing

Added comprehensive tests in `FunctionMetadataCatalogFilterTest.cpp`:

- `TestMultipleNamespaces`: Verifies that functions are properly registered under multiple catalogs
- Enhanced existing tests to cover additional namespace scenarios

## Files Modified

1. `presto_cpp/main/common/Configs.h` - Added configuration constant
2. `presto_cpp/main/common/Configs.cpp` - Implemented configuration parsing
3. `presto_cpp/main/PrestoServer.h` - Added member variable for additional prefixes
4. `presto_cpp/main/PrestoServer.cpp` - Modified function registration logic
5. `presto_cpp/main/types/tests/FunctionMetadataCatalogFilterTest.cpp` - Enhanced tests

## Migration Guide

### For Existing Deployments

No changes required - the feature is backward compatible.

### For New Multiple Namespace Deployments

1. Add `presto.additional-namespace-prefixes` to your `config.properties`
2. Configure multiple function namespace managers in your coordinator
3. Update your SQL queries to use qualified function names if needed

## Future Enhancements

1. **Dynamic Registration**: Support for registering/unregistering namespaces at runtime
2. **Namespace Metadata**: Add endpoint to list available namespaces
3. **Function Versioning**: Support for different function versions across namespaces
4. **Access Control**: Integration with Presto's security model for namespace-level permissions
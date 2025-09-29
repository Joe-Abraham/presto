# Native Sidecar Function Registry with Multiple Catalogs

This document describes the enhanced native sidecar function registry that supports registering functions under different catalogs and schemas.

## Overview

The native sidecar function registry now supports:
1. **Catalog-filtered endpoints**: Functions can be retrieved by specific catalog
2. **Multiple namespace registrations**: Different sidecar endpoints can serve different catalogs
3. **Hive-specific functions**: When enabled, Hive functions like `initcap` are available under the `hive` catalog

## Configuration

### C++ Sidecar Configuration

Add the following to your `config.properties` file:

```properties
# Enable sidecar mode
native-sidecar=true

# Enable Hive-specific functions (optional)
sidecar.enable-hive-functions=true
```

### Java Plugin Configuration

Configure multiple function namespace managers for different catalogs:

```properties
# For Hive functions
function-namespace-manager.name=native-sidecar
catalog=hive

# For built-in functions (separate configuration)
function-namespace-manager.name=native-sidecar  
catalog=presto.default
```

## API Endpoints

The C++ sidecar now provides the following endpoints:

### Get All Functions
```
GET /v1/functions
```
Returns functions from all catalogs.

### Get Functions by Catalog
```
GET /v1/functions/{catalog}
```
Returns functions only from the specified catalog.

Examples:
- `GET /v1/functions/hive` - Returns only Hive functions
- `GET /v1/functions/presto.default` - Returns only built-in functions

## Usage Examples

### Multiple Sidecar Endpoints

You can register multiple sidecar endpoints with different catalogs:

1. **Hive Function Sidecar** (port 7778):
   ```properties
   catalog=hive
   sidecar.enable-hive-functions=true
   ```

2. **Custom Function Sidecar** (port 7779):
   ```properties
   catalog=custom
   ```

### Querying Functions

```sql
-- Use Hive initcap function
SELECT hive.default.initcap('hello world');

-- Use built-in abs function  
SELECT presto.default.abs(-42);

-- Mix functions from different catalogs
SELECT hive.default.initcap(name), presto.default.abs(value) 
FROM my_table;
```

## Testing

### Unit Tests

The implementation includes tests to verify:
- Catalog filtering works correctly
- Multiple namespaces can coexist
- Functions are properly separated by catalog

Run tests:
```bash
# C++ tests
cd presto-native-execution
make test

# Java tests
mvn test -pl presto-native-sidecar-plugin
```

### Integration Testing

1. Start a sidecar with Hive functions enabled
2. Configure Java plugin to use "hive" catalog
3. Verify functions are available under correct namespace
4. Test multiple sidecar endpoints with different catalogs

## Benefits

1. **Function Namespacing**: Custom functions don't conflict with built-ins
2. **Catalog Isolation**: Functions are properly isolated by catalog
3. **Scalability**: Multiple sidecar processes can serve different function sets
4. **Compatibility**: Hive-specific functions work seamlessly with existing queries

## Implementation Details

### C++ Side
- `HiveFunctionRegistration.cpp`: Registers Hive functions with proper namespacing
- `getFunctionsMetadataForCatalog()`: Filters functions by catalog
- Configuration through `SystemConfig::sidecarEnableHiveFunctions()`

### Java Side  
- `NativeFunctionDefinitionProvider`: Handles catalog-specific endpoint calls
- `NativeFunctionNamespaceManagerConfig`: Supports catalog configuration
- Backward compatible with existing empty catalog configuration

### Function Registration
Functions are registered with full namespace qualification:
```cpp
velox::exec::registerVectorFunction(
    "hive.default.initcap",  // Full namespace: catalog.schema.function
    signatures,
    implementation);
```

This enables proper catalog-based filtering and prevents naming conflicts.
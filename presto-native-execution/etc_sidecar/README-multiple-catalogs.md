# Multiple Catalog Support for Native Sidecar Functions

This directory contains configuration files to test multiple catalog support for native sidecar function registry.

## Overview

The native sidecar function registry can now support registering functions under different catalogs and schemas, not just the built-in namespace. This allows users to namespace their custom C++ functions separately from built-in functions.

## Configuration Files

### Function Namespace Managers

The `function-namespace/` directory contains configuration files for different catalogs:

- **`native.properties`** - Default native catalog for built-in functions
- **`custom_cpp.properties`** - Custom C++ functions catalog  
- **`user_functions.properties`** - User-defined functions catalog

Each configuration file specifies:
```properties
function-namespace-manager.name=native
native-sidecar.num-retries=3
native-sidecar.retry-delay=1s
```

### Example Configuration

The file `multi-catalog-config.properties.example` shows how to configure the main server to support multiple catalogs.

## Testing Multiple Catalog Support

To test multiple catalog support:

1. **Start the native sidecar** with functions organized by catalog
2. **Configure Presto server** to use multiple function namespace managers
3. **Query functions** using the catalog.schema.function syntax:
   - `native.default.my_function`
   - `custom_cpp.analytics.cpp_function`
   - `user_functions.custom.user_function`

## Expected Behavior

With multiple catalog support:

1. **Function Registration**: Functions can be registered under different catalogs
2. **Namespace Isolation**: Functions in different catalogs are isolated from each other
3. **Catalog-Filtered Queries**: The `/v1/functions/{catalog}` endpoint filters functions by catalog
4. **Multiple Managers**: Multiple native sidecar endpoints can be registered with different namespaces

## Implementation Notes

This configuration enables testing of:

- Multiple `NativeFunctionNamespaceManager` instances with different catalog names
- Catalog-aware function discovery and registration
- Function namespace isolation similar to existing SQL function namespace managers
- Support for the planned `/v1/functions/{catalog}` endpoint in the C++ sidecar

## Files Structure

```
etc_sidecar/
├── config.properties                           # Main sidecar configuration
├── multi-catalog-config.properties.example     # Example multi-catalog config
├── function-namespace/                          # Function namespace configurations
│   ├── native.properties                       # Native catalog config
│   ├── custom_cpp.properties                   # Custom C++ catalog config
│   └── user_functions.properties               # User functions catalog config
├── catalog/                                     # Connector catalogs
│   ├── hive.properties
│   ├── iceberg.properties
│   └── tpchstandard.properties
└── README-multiple-catalogs.md                 # This documentation
```
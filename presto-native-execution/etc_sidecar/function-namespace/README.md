# Native Sidecar Function Namespace Configurations

This directory contains configuration files for different native sidecar function namespace scenarios.

## Configuration Files

### native.properties
- **Purpose**: Provides access to all functions from the native sidecar
- **Usage**: Default configuration that returns functions from all catalogs
- **Catalog Filtering**: Disabled (empty sidecar.catalog-name)

### native-custom.properties  
- **Purpose**: Provides access only to functions from a custom catalog
- **Usage**: Template for custom catalog filtering (will be empty unless custom functions are registered)
- **Catalog Filtering**: Enabled for custom catalog (as example)

### native-presto.properties
- **Purpose**: Provides access only to functions from the presto catalog  
- **Usage**: Use when you want to filter functions to only those with "presto.default.*" namespace
- **Catalog Filtering**: Enabled for "presto" catalog

## How to Use

1. **Copy the desired configuration** to your coordinator's `etc/function-namespace/` directory
2. **Rename it** to match your desired namespace name (e.g., `native-functions.properties`)
3. **Ensure the native sidecar is running** and accessible from the coordinator
4. **Restart the coordinator** to load the new function namespace manager

## Configuration Properties

- `function-namespace-manager.name`: Must be "native" for the native sidecar plugin
- `supported-function-languages`: Set to "CPP" for C++ functions
- `function-implementation-type`: Set to "CPP" for native implementations  
- `sidecar.catalog-name`: Optional property to filter functions by catalog
  - Leave empty or omit for all functions
  - Set to "hive" for hive catalog functions only
  - Set to "presto" for presto catalog functions only
  - Set to custom catalog name for custom functions

## Example Usage

### All Functions (Default)
```properties
function-namespace-manager.name=native
supported-function-languages=CPP
function-implementation-type=CPP
```

### Presto/Native Functions Only
```properties  
function-namespace-manager.name=native
supported-function-languages=CPP
function-implementation-type=CPP
sidecar.catalog-name=presto
```

### Custom Catalog Functions
```properties
function-namespace-manager.name=native
supported-function-languages=CPP
function-implementation-type=CPP
sidecar.catalog-name=my_custom_catalog
```

## Verification

After configuration, you can verify the setup by:

1. **Connecting to the coordinator** with the Presto CLI
2. **Running** `SHOW FUNCTIONS;` to see available functions
3. **Checking** that function names have the expected catalog namespace (e.g., `hive.default.initcap`)

## Multiple Catalogs

To register multiple native sidecar function namespaces with different catalogs:

1. Create multiple configuration files with different names
2. Each should have different `sidecar.catalog-name` values
3. Place them all in the coordinator's `etc/function-namespace/` directory
4. Restart the coordinator

Example:
- `etc/function-namespace/native-all.properties` (no catalog filtering)
- `etc/function-namespace/native-presto.properties` (presto catalog only)
- `etc/function-namespace/native-custom.properties` (custom catalog only)
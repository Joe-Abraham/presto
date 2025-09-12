# Multiple Namespace Support for Native Sidecar

## Overview

The native sidecar now supports multiple function namespaces, allowing functions from different catalogs to be registered in their appropriate namespaces. This provides better isolation and organization of functions.

## Configuration

### Function Namespace Configuration

Function namespaces are configured using properties files in the `etc_sidecar/function-namespace/` directory. Each file represents a separate namespace.

#### Example: Native Namespace (`native.properties`)
```properties
function-namespace-manager.name=native
catalog=native
```

#### Example: Hive Namespace (`hive.properties`) 
```properties
function-namespace-manager.name=native
catalog=hive
```

### Main Configuration

The main configuration file (`etc_sidecar/config.properties`) specifies:

```properties
# Default namespace for unqualified function calls
presto.default-namespace=native.default

# Directory containing function namespace configurations
function-namespace.config-dir=etc_sidecar/function-namespace/
```

## Default Namespace Behavior

The `presto.default-namespace=native.default` setting determines how unqualified function calls are resolved:

- **With single namespace**: All unqualified functions resolve to `native.default`
- **With multiple namespaces**: Unqualified functions still resolve to `native.default`, but qualified calls like `hive.default.initcap()` are supported

## Multiple Namespace Benefits

1. **Function Isolation**: Functions from different catalogs are isolated in their own namespaces
2. **Qualified Function Calls**: Support for `catalog.schema.function()` syntax
3. **Catalog-Specific Functions**: Hive functions only appear when hive catalog is configured
4. **Better Organization**: Clear separation between built-in and catalog-specific functions

## Namespace Resolution

Function resolution follows this precedence:
1. Fully qualified: `catalog.schema.function()` → looks in specified catalog namespace
2. Schema qualified: `schema.function()` → looks in default namespace 
3. Unqualified: `function()` → looks in default namespace (`native.default`)

## Adding New Namespaces

To add a new namespace:

1. Create a properties file in `etc_sidecar/function-namespace/`
2. Set the `function-namespace-manager.name=native` 
3. Set the `catalog=<catalog_name>` to filter functions from sidecar
4. The namespace will be automatically loaded as `<filename>.default`

## Constraints and Considerations

- The single `presto.default-namespace=native.default` means unqualified functions always resolve to the default namespace
- For true multi-catalog default resolution, you would need multiple default namespace configurations
- The current design maintains backward compatibility while enabling multiple qualified namespaces
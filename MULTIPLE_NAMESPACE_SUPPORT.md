# Multiple Namespace Support for Native Sidecar

## Overview

The native sidecar now supports multiple function namespaces, allowing functions from different catalogs to be registered in their appropriate namespaces. This provides better isolation and organization of functions.

## Configuration

### Function Namespace Configuration

Function namespaces are configured directly in the main configuration file using the standard Presto namespace manager pattern.

### Main Configuration

The main configuration file (`etc_sidecar/config.properties`) specifies:

```properties
# Default namespace for unqualified function calls
presto.default-namespace=native.default

# Native functions namespace (built-in Presto functions)
function-namespace-manager.native.name=native
function-namespace-manager.native.catalog=native

# Hive functions namespace (Hive-specific functions)
function-namespace-manager.hive.name=native
function-namespace-manager.hive.catalog=hive
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

## Function Namespaces Created

With the above configuration, the following namespaces are created:
- **native.default**: Contains native Presto functions (abs, substring, etc.)
- **hive.default**: Contains Hive-specific functions (initcap, etc.)

## Usage Examples

```sql
-- Unqualified function call (resolves to native.default)
SELECT abs(-5);

-- Qualified function call to hive namespace
SELECT hive.default.initcap('hello world');

-- Schema-qualified call (resolves to default namespace)  
SELECT default.abs(-10);
```

## Adding New Namespaces

To add a new namespace:

1. Add a new function namespace manager configuration in `etc_sidecar/config.properties`
2. Set the appropriate factory name and catalog filter
3. The namespace will be automatically loaded as `<namespace_name>.default`

Example for adding a custom catalog:
```properties
function-namespace-manager.mycatalog.name=native
function-namespace-manager.mycatalog.catalog=mycatalog
```

## Constraints and Considerations

- The single `presto.default-namespace=native.default` means unqualified functions always resolve to the default namespace
- Each namespace manager uses the same factory (`native`) but filters functions by catalog
- The current design maintains backward compatibility while enabling multiple qualified namespaces
- Functions must be registered in the sidecar with the appropriate catalog prefix (e.g., `hive.default.initcap`)
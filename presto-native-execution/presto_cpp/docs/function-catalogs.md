# Function Catalogs in Presto Native

## Overview

Function catalogs provide a way to organize related dynamic functions with shared configuration in Presto Native, similar to how connector catalogs work. This feature enables:

- **Organization**: Group related functions into logical catalogs
- **Shared Configuration**: Multiple functions can share common configuration properties
- **Session-Level Overrides**: Users can override catalog configuration at runtime via session properties
- **Modular Design**: Functions can be organized by provider, functionality, or domain

## Configuration

### Creating a Function Catalog

Function catalogs are configured using `.properties` files in the `function-catalog` directory within your configuration directory (typically `etc/function-catalog/`).

Example: `etc/function-catalog/hive.properties`

```properties
# Hive Function Catalog Configuration
# Configuration properties for Hive-specific functions

# Optional: Mark this explicitly as a function catalog
# connector.name=function-catalog

# Add catalog-specific properties
hive.function.case-sensitive=false
hive.function.locale=en_US
```

### Directory Structure

```
etc/
├── catalog/              # Connector catalogs
│   ├── hive.properties
│   └── iceberg.properties
├── function-catalog/     # Function catalogs (new)
│   ├── hive.properties
│   └── ai.properties
├── config.properties
└── node.properties
```

## Usage

### Registering Functions in a Catalog

Functions can be registered within a catalog using the `registerCatalogFunction` helper:

```cpp
#include "presto_cpp/main/functions/dynamic_registry/CatalogFunctionRegistrar.h"

// Register a custom function in a catalog under the 'default' schema
facebook::presto::registerCatalogFunction<MyFunction, Varchar, Varchar>(
    "custom",    // catalog name
    "default",   // schema name
    "myfunction" // function name
);
// This registers the function as: custom.default.myfunction
```

### Accessing Catalog Configuration

Functions can access their catalog's configuration at runtime:

```cpp
#include "presto_cpp/main/functions/FunctionCatalogManager.h"

// Get catalog configuration
auto* catalogManager = FunctionCatalogManager::instance();
auto config = catalogManager->getCatalogConfig("custom");

if (config) {
  // Access configuration properties
  auto caseSensitive = config->optionalProperty<bool>("custom.case-sensitive");
  auto locale = config->propertyOrDefault<std::string>("custom.locale", "en_US");
}
```

### Session Property Overrides

Users can override catalog configuration at runtime using session properties. The session properties are merged with the catalog's base configuration:

```cpp
// Get configuration with session overrides
std::unordered_map<std::string, std::string> sessionProps;
sessionProps["api.key"] = "user-specific-key";

auto configWithSession = catalogManager->getCatalogConfigWithSession(
    "ai", sessionProps);
```

## Example Use Cases

### 1. AI Functions with API Keys

```properties
# etc/function-catalog/ai.properties
ai.openai.api-key=default-key
ai.openai.model=gpt-4
ai.openai.max-tokens=1000
```

Functions in the AI catalog can access these shared properties, and users can override them per-session with their own API keys.

### 2. Custom String Functions

```properties
# etc/function-catalog/custom-strings.properties
strings.default-encoding=UTF-8
strings.trim-whitespace=true
```

### 3. Geospatial Functions

```properties
# etc/function-catalog/geo.properties
geo.default-srid=4326
geo.precision=6
```

## Benefits

1. **Configuration Management**: Centralized configuration for related functions
2. **Security**: API keys and credentials can be managed at the catalog level
3. **Flexibility**: Different users/sessions can use different configurations
4. **Organization**: Functions are logically grouped, making them easier to discover and manage
5. **Consistency**: Similar to connector catalogs, providing a familiar pattern

## Implementation Notes

- Function catalogs are loaded during server startup from the `function-catalog` directory
- Catalog configurations are managed by `FunctionCatalogManager` (singleton)
- Functions are registered with fully qualified names: `catalog.schema.function`
- Session properties are merged on-demand and don't modify the base catalog configuration
- Multiple catalogs can coexist, each with its own configuration namespace

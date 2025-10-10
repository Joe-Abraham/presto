# RFC: Catalog-Filtered Functions in Native Sidecar Function Registry

## Status
**Implemented** (as of commit 3767a71)

## Authors
- Presto Native Execution Team

## Overview

This RFC describes the design and implementation of catalog-filtered function registration in the native sidecar function registry. This feature enables namespace separation for C++ functions, allowing users to register custom functions under different catalogs and schemas, separate from built-in functions.

## Background

### Problem Statement

Prior to this change, the native sidecar function registry bundled all functions together regardless of catalog, making it impossible to:
- Namespace custom C++ functions separately from built-in functions
- Register multiple native sidecar endpoints with different non-built-in namespaces
- Maintain proper isolation between function catalogs
- Achieve feature parity with Java function namespace managers

This limitation prevented users from organizing their custom C++ functions in a way that mirrors the capabilities available in Java-based function namespace managers, as documented in the [Presto Function Namespace Managers documentation](https://prestodb.io/docs/current/admin/function-namespace-managers.html).

### Expected Behavior

The native sidecar function registry should support:
1. Registering functions under different catalogs and schemas
2. Filtering function metadata by catalog when queried
3. Proper isolation to prevent cross-catalog function leakage
4. Multiple function namespace manager instances with different catalogs

## Design

### Architecture

The solution introduces catalog-based filtering at three layers:

```
┌─────────────────────────────────────────────────────────────┐
│                    Presto Coordinator                        │
│  ┌────────────────────────────────────────────────────────┐ │
│  │   NativeFunctionNamespaceManager (Catalog: mycatalog)  │ │
│  │   NativeFunctionNamespaceManager (Catalog: presto)     │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                           │
                           │ HTTP GET /v1/functions/{catalog}
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Presto Native Sidecar (C++ Worker)              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  PrestoServer HTTP Endpoints                           │ │
│  │  - GET /v1/functions         (all functions)           │ │
│  │  - GET /v1/functions/{catalog} (filtered by catalog)   │ │
│  └────────────────────────────────────────────────────────┘ │
│                           │                                  │
│                           ▼                                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  getFunctionsMetadata(catalog)                         │ │
│  │  - Filters scalar functions by catalog                 │ │
│  │  - Filters aggregate functions by catalog              │ │
│  │  - Filters window functions by catalog                 │ │
│  └────────────────────────────────────────────────────────┘ │
│                           │                                  │
│                           ▼                                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Velox Function Registry                               │ │
│  │  Functions registered as: catalog.schema.function_name │ │
│  │  Example: mycatalog.myschema.my_custom_function        │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. C++ Backend: HTTP Endpoint (PrestoServer.cpp)

**Location:** `presto-native-execution/presto_cpp/main/PrestoServer.cpp:1672-1683`

**Implementation:**
```cpp
httpServer_->registerGet(
    R"(/v1/functions/([^/]+))",
    [](proxygen::HTTPMessage* /*message*/,
       const std::vector<std::string>& pathMatch) {
      return new http::CallbackRequestHandler(
          [catalog = pathMatch[1]](
              proxygen::HTTPMessage* /*message*/,
              std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
              proxygen::ResponseHandler* downstream) {
            http::sendOkResponse(downstream, getFunctionsMetadata(catalog));
          });
    });
```

**Design Decisions:**
- Uses regex pattern `([^/]+)` to capture catalog name from URL path
- Delegates filtering logic to `getFunctionsMetadata(catalog)` function
- Returns JSON response with filtered function metadata

#### 2. C++ Backend: Filtering Logic (FunctionMetadata.cpp)

**Location:** `presto-native-execution/presto_cpp/main/functions/FunctionMetadata.cpp:322-382`

**Implementation:**
```cpp
json getFunctionsMetadata(const std::string& catalog) {
  json j;
  
  // Filter scalar functions
  const auto signatures = getFunctionSignatures();
  for (const auto& entry : signatures) {
    const auto name = entry.first;
    const auto parts = getFunctionNameParts(name);
    if (parts[0] != catalog) {
      continue;  // Skip functions from other catalogs
    }
    const auto schema = parts[1];
    const auto function = parts[2];
    j[function] = buildScalarMetadata(name, schema, entry.second);
  }
  
  // Filter aggregate functions (similar logic)
  // Filter window functions (similar logic)
  
  return j;
}
```

**Function Naming Convention:**
Functions in Velox are registered with fully qualified names: `catalog.schema.function_name`

For example:
- `presto.default.abs` (built-in function)
- `mycatalog.myschema.my_custom_function` (custom function)

The `getFunctionNameParts()` utility splits the function name into three components:
1. `parts[0]` = catalog (e.g., "presto", "mycatalog")
2. `parts[1]` = schema (e.g., "default", "myschema")
3. `parts[2]` = function name (e.g., "abs", "my_custom_function")

**Filtering Strategy:**
- Iterates through all registered functions (scalar, aggregate, window)
- Extracts catalog from fully qualified function name
- Includes only functions matching the requested catalog
- Handles internal function blocklist consistently

#### 3. Java Plugin: Catalog-Scoped HTTP Client

**Location:** `presto-native-sidecar-plugin/src/main/java/com/facebook/presto/sidecar/functionNamespace/NativeFunctionDefinitionProvider.java:64-82`

**Implementation:**
```java
@Override
public UdfFunctionSignatureMap getUdfDefinition(NodeManager nodeManager) {
    try {
        URI baseUri = getSidecarLocationOnStartup(
                nodeManager, config.getSidecarNumRetries(), config.getSidecarRetryDelay().toMillis());
        // Catalog-filtered endpoint: /v1/functions/{catalog}
        URI catalogUri = HttpUriBuilder.uriBuilderFrom(baseUri).appendPath(catalogName).build();
        Request catalogRequest = prepareGet().setUri(catalogUri).build();
        Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap =
                httpClient.execute(catalogRequest, createJsonResponseHandler(nativeFunctionSignatureMapJsonCodec));
        return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
    }
    catch (Exception e) {
        // Do not fall back to unfiltered endpoint to avoid cross-catalog leakage
        throw new PrestoException(INVALID_ARGUMENTS, 
            String.format("Failed to get catalog-scoped functions from sidecar for catalog '%s'", catalogName), e);
    }
}
```

**Design Decisions:**
- Each `NativeFunctionNamespaceManager` instance is bound to a specific catalog
- Catalog name is injected via `@ServingCatalog` annotation
- No fallback to unfiltered endpoint to prevent cross-catalog leakage
- Clear error messages when catalog-scoped functions cannot be fetched

#### 4. Module Configuration

**Location:** `presto-native-sidecar-plugin/src/main/java/com/facebook/presto/sidecar/functionNamespace/NativeFunctionNamespaceManagerModule.java`

**Implementation:**
```java
@Override
public void configure(Binder binder) {
    binder.bind(new TypeLiteral<String>() {})
          .annotatedWith(ServingCatalog.class)
          .toInstance(catalogName);
    // ... other bindings
}
```

**Design Decisions:**
- Catalog name is provided during module instantiation
- Injected into `NativeFunctionDefinitionProvider` via dependency injection
- Allows multiple function namespace managers with different catalogs

## API Design

### Endpoint Specification

#### GET /v1/functions

Returns all functions from all catalogs.

**Response Format:**
```json
{
  "function_name": [
    {
      "outputType": "bigint",
      "paramTypes": ["bigint"],
      "schema": "default",
      "functionKind": "SCALAR",
      "routineCharacteristics": { ... }
    }
  ]
}
```

#### GET /v1/functions/{catalog}

Returns only functions from the specified catalog.

**Path Parameters:**
- `catalog` (string, required): The catalog name to filter by (e.g., "presto", "mycatalog")

**Response Format:**
Same as `/v1/functions` but filtered to include only functions where the fully qualified name starts with `{catalog}.`

**Example Request:**
```
GET /v1/functions/mycatalog
```

**Example Response:**
```json
{
  "my_custom_function": [
    {
      "outputType": "varchar",
      "paramTypes": ["varchar"],
      "schema": "myschema",
      "functionKind": "SCALAR",
      "routineCharacteristics": {
        "language": {"languageName": "CPP"},
        "determinism": "DETERMINISTIC",
        "nullCallClause": "RETURNS_NULL_ON_NULL_INPUT"
      }
    }
  ]
}
```

### OpenAPI Specification

This implementation aligns with the specification defined in [rest_function_server.yaml](https://github.com/prestodb/presto/blob/master/presto-openapi/src/main/resources/rest_function_server.yaml#L44-L64):

```yaml
/v1/functions/{catalog}:
  get:
    summary: Get functions by catalog
    parameters:
      - name: catalog
        in: path
        required: true
        schema:
          type: string
    responses:
      '200':
        description: Function metadata filtered by catalog
```

## Implementation Details

### Function Registration

Functions must be registered in Velox with fully qualified names following the pattern: `catalog.schema.function_name`

**Example (C++):**
```cpp
// Register a custom function in "mycatalog.myschema" namespace
registerFunction<MyCustomFunction, Varchar, Varchar>(
    {"mycatalog.myschema.my_custom_function"}
);
```

**Example (using DynamicFunctionRegistrar):**
```cpp
registerPrestoFunction<MyCustomFunction, TReturn, TArgs...>(
    "my_custom_function",    // function name
    "mycatalog.myschema",    // namespace (catalog.schema)
    constraints
);
```

### Error Handling

The implementation includes robust error handling:

1. **Invalid Catalog:** Returns empty result set (empty JSON object)
2. **Network Errors:** Throws `PrestoException` with clear error message
3. **No Fallback:** Does not fall back to unfiltered endpoint to prevent cross-catalog leakage

### Testing

**Location:** `presto-native-execution/presto_cpp/main/functions/tests/FunctionMetadataTest.cpp:117-158`

**Test Coverage:**
1. **GetFunctionsMetadataWithCatalog:** Validates filtering by valid catalog
   - Verifies JSON structure is correct
   - Confirms only functions from specified catalog are returned
   - Checks that schema field is correctly extracted

2. **GetFunctionsMetadataWithNonExistentCatalog:** Validates handling of non-existent catalog
   - Confirms empty result set for catalog with no functions
   - Ensures no errors are thrown for valid but empty catalogs

## Usage Examples

### Configuration

#### Coordinator Configuration

**etc/config.properties:**
```properties
coordinator-sidecar-enabled=true
native-execution-enabled=true
presto.default-namespace=native.default
plugin.dir={root-directory}/native-plugins/
```

#### Function Namespace Manager Configuration

**etc/function-namespace/mycatalog.properties:**
```properties
function-namespace-manager.name=native
function-implementation-type=CPP
supported-function-languages=CPP
```

#### Sidecar Worker Configuration

**etc/config.properties:**
```properties
native-sidecar=true
presto.default-namespace=native.default
```

### Multiple Catalogs

You can register multiple function namespace managers, each with different catalogs:

**etc/function-namespace/catalog1.properties:**
```properties
function-namespace-manager.name=native
function-implementation-type=CPP
supported-function-languages=CPP
```

**etc/function-namespace/catalog2.properties:**
```properties
function-namespace-manager.name=native
function-implementation-type=CPP
supported-function-languages=CPP
```

Each manager will fetch only the functions registered under its respective catalog, maintaining proper isolation.

### SQL Query Example

```sql
-- Using built-in Presto functions (from 'presto' catalog)
SELECT presto.default.abs(-5);

-- Using custom functions from 'mycatalog'
SELECT mycatalog.myschema.my_custom_function('hello');

-- Functions are isolated by catalog
-- This would fail if my_custom_function is not in the presto catalog
SELECT presto.default.my_custom_function('hello');  -- Error
```

## Security Considerations

### Cross-Catalog Leakage Prevention

The implementation includes several safeguards to prevent cross-catalog function leakage:

1. **No Fallback:** The Java plugin does not fall back to the unfiltered `/v1/functions` endpoint if the catalog-filtered endpoint fails
2. **Explicit Filtering:** All filtering is done explicitly at the C++ level based on function name prefixes
3. **Error Propagation:** Clear error messages indicate when catalog-scoped functions cannot be fetched

### Authentication and Authorization

This implementation does not change the authentication/authorization model. All existing security mechanisms remain in place:
- HTTP endpoint authentication follows existing patterns
- Function execution permissions are managed at the Presto coordinator level
- No additional privileges are required for catalog-filtered endpoints

## Performance Considerations

### Filtering Performance

The catalog filtering operation has minimal performance impact:

1. **O(n) complexity:** Filters through all registered functions once
2. **Lazy evaluation:** Filtering happens only when endpoint is called
3. **No caching overhead:** Results are not cached (same as unfiltered endpoint)
4. **Memory efficient:** Only matching functions are included in response

### Network Efficiency

The catalog-filtered endpoint improves network efficiency:
- Reduces payload size by returning only relevant functions
- Eliminates need for client-side filtering
- Fewer functions to serialize and deserialize

## Alternatives Considered

### Alternative 1: Client-Side Filtering

**Description:** Keep the existing `/v1/functions` endpoint and filter functions on the Java side.

**Rejected because:**
- Increases network payload size
- Adds latency due to unnecessary data transfer
- Requires duplicate filtering logic in Java
- Higher memory usage on both client and server

### Alternative 2: Separate Endpoints per Catalog

**Description:** Register a separate endpoint for each catalog (e.g., `/v1/functions/presto`, `/v1/functions/mycatalog`).

**Rejected because:**
- Not scalable (requires endpoint registration for each catalog)
- Harder to maintain
- Less flexible for dynamic catalog addition
- Does not align with REST conventions

### Alternative 3: Query Parameter Filtering

**Description:** Use query parameters (e.g., `/v1/functions?catalog=mycatalog`) instead of path parameters.

**Rejected because:**
- Path parameters are more RESTful for resource identification
- Harder to version and cache
- Does not align with existing OpenAPI specification

## Migration and Compatibility

### Backward Compatibility

This change is **fully backward compatible**:

1. **Existing Endpoint Preserved:** The `/v1/functions` endpoint continues to work without changes
2. **No Breaking Changes:** Existing function namespace managers using the unfiltered endpoint are unaffected
3. **Opt-in:** Catalog filtering is only used when the new endpoint is explicitly called

### Migration Path

No migration is required for existing deployments. To adopt catalog-filtered functions:

1. Update native sidecar plugin to version with catalog filtering support
2. Configure function namespace managers with appropriate catalog names
3. Register C++ functions with fully qualified names including desired catalog

## Future Work

### Potential Enhancements

1. **Schema-Level Filtering:** Add `/v1/functions/{catalog}/{schema}` endpoint for finer-grained filtering
2. **Configurable Blocklist:** Make the internal function blocklist configurable via configuration file
3. **Function Name Filtering:** Add `/v1/functions/{catalog}/{schema}/{function_name}` for individual function lookup
4. **Caching:** Implement caching layer for frequently accessed function metadata
5. **Metrics:** Add metrics for catalog-filtered endpoint usage

### Known Limitations

1. **Path Ambiguity:** The `/v1/functions/{catalog}` endpoint could conflict with future `/v1/functions/{schema}` endpoint if schema-level filtering is added without the catalog prefix
2. **Static Blocklist:** The internal function blocklist is hard-coded and duplicated between filtered and unfiltered implementations

## References

1. [Presto Function Namespace Managers Documentation](https://prestodb.io/docs/current/admin/function-namespace-managers.html)
2. [OpenAPI Specification for Function Server](https://github.com/prestodb/presto/blob/master/presto-openapi/src/main/resources/rest_function_server.yaml)
3. [Native Sidecar Plugin Documentation](https://prestodb.io/docs/current/plugin/native-sidecar-plugin.html)

## Appendix

### Implementation Commit

The feature was implemented in commit `3767a71` with the following changes:

- `presto-native-execution/presto_cpp/main/PrestoServer.cpp`: Added catalog-filtered endpoint
- `presto-native-execution/presto_cpp/main/functions/FunctionMetadata.cpp`: Implemented filtering logic
- `presto-native-execution/presto_cpp/main/functions/FunctionMetadata.h`: Added function declaration
- `presto-native-sidecar-plugin/.../NativeFunctionDefinitionProvider.java`: Updated to call catalog-filtered endpoint
- `presto-native-execution/presto_cpp/main/functions/tests/FunctionMetadataTest.cpp`: Added test coverage

### Documentation Updates

Documentation was added in subsequent commits to document the new endpoint in:
- `presto-docs/src/main/sphinx/presto_cpp/sidecar.rst`

## Conclusion

The catalog-filtered functions feature successfully enables namespace separation in the native sidecar function registry, achieving feature parity with Java function namespace managers. The implementation is backward compatible, well-tested, and follows REST API best practices. It provides a solid foundation for organizing custom C++ functions in production Presto deployments.

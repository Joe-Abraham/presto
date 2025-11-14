# Remote Function Execution Design Document

## Overview

This design document describes the implementation of remote function execution support in Presto native workers. The feature enables Presto to delegate function execution to external REST/HTTP services, providing extensibility without modifying the core engine.

## Motivation

### Goals
- Enable execution of custom functions hosted on external services
- Provide a standard REST-based protocol for remote function invocation
- Maintain compatibility with existing Velox function infrastructure
- Support multiple serialization formats (Presto Page, Spark UnsafeRow)
- Ensure thread-safe registration and execution in multi-threaded environments

### Non-Goals
- This feature does not replace local function execution
- Does not provide automatic service discovery or load balancing
- Does not include built-in authentication/authorization mechanisms

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        Presto Native Worker                      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │            Expression Evaluation Engine                     │ │
│  │                                                              │ │
│  │  ┌─────────────────────────┐                                │ │
│  │  │ PrestoToVeloxExpr       │                                │ │
│  │  │ (Expression Converter)  │                                │ │
│  │  └──────────┬──────────────┘                                │ │
│  │             │ Detects RestFunctionHandle                    │ │
│  │             ▼                                                │ │
│  │  ┌─────────────────────────┐                                │ │
│  │  │ PrestoRestFunction      │                                │ │
│  │  │ Registration            │                                │ │
│  │  │ - Parses function ID    │                                │ │
│  │  │ - Builds signatures     │                                │ │
│  │  │ - Thread-safe registry  │                                │ │
│  │  └──────────┬──────────────┘                                │ │
│  │             │ Registers with                                 │ │
│  │             ▼                                                │ │
│  │  ┌─────────────────────────┐                                │ │
│  │  │ RestRemoteFunction      │                                │ │
│  │  │ (Velox VectorFunction)  │                                │ │
│  │  │ - Wraps REST client     │                                │ │
│  │  │ - Handles serialization │                                │ │
│  │  └──────────┬──────────────┘                                │ │
│  └─────────────┼──────────────────────────────────────────────┘ │
│                │                                                  │
│                ▼                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │            RestRemoteClient                                 │ │
│  │  - HTTP client (Proxygen)                                   │ │
│  │  - Connection pooling                                       │ │
│  │  - Timeout management                                       │ │
│  │  - HTTPS support                                            │ │
│  └──────────────┬─────────────────────────────────────────────┘ │
└─────────────────┼─────────────────────────────────────────────┘
                  │ HTTP POST with serialized data
                  ▼
       ┌─────────────────────────┐
       │  Remote Function Server │
       │  - HTTP REST endpoint   │
       │  - Deserializes request │
       │  - Executes function    │
       │  - Serializes response  │
       └─────────────────────────┘
```

### Component Details

#### 1. Expression Integration (`PrestoToVeloxExpr.cpp`)

**Purpose**: Intercepts `RestFunctionHandle` during expression conversion and triggers registration.

**Key Logic**:
```cpp
if (auto restFunctionHandle = 
    std::dynamic_pointer_cast<protocol::RestFunctionHandle>(
        pexpr.functionHandle)) {
    // Register the remote function with Velox
    functions::remote::rest::registerRestRemoteFunction(*restFunctionHandle);
    
    // Create standard CallTypedExpr
    return std::make_shared<CallTypedExpr>(
        returnType, args, getFunctionName(restFunctionHandle->functionId));
}
```

**Thread Safety**: Registration uses double-checked locking pattern to ensure safe concurrent access.

#### 2. Function Registration (`PrestoRestFunctionRegistration.cpp/h`)

**Purpose**: Parses Presto function metadata and registers with Velox's function registry.

**Key Responsibilities**:
- Parse `SqlFunctionId` to extract function name (format: `namespace.schema.function;TYPE;TYPE`)
- Convert Presto `Signature` to Velox `FunctionSignaturePtr`
- Maintain static registries for:
  - Registered function handles (avoid duplicate registration)
  - REST client instances (one per location URL)
- Thread-safe access using `std::mutex`

**Implementation Details**:
```cpp
void registerRestRemoteFunction(
    const protocol::RestFunctionHandle& restFunctionHandle) {
    
    static std::mutex mutex;
    static std::unordered_set<std::string> registeredFunctionHandles;
    static std::unordered_map<std::string, RestRemoteClientPtr> remoteClients;
    
    std::lock_guard<std::mutex> lock(mutex);
    
    // Check if already registered
    auto key = generateKey(restFunctionHandle);
    if (registeredFunctionHandles.contains(key)) {
        return;
    }
    
    // Get or create REST client for this location
    auto& client = remoteClients[restFunctionHandle.location];
    if (!client) {
        client = std::make_shared<RestRemoteClient>(restFunctionHandle.location);
    }
    
    // Convert signatures and register
    VeloxRemoteFunctionMetadata metadata{
        .location = buildFullUrl(restFunctionHandle),
        .serdeFormat = getSerdeFormat()
    };
    
    registerVeloxRemoteFunction(name, signatures, metadata, client);
    registeredFunctionHandles.insert(key);
}
```

**URL Construction**:
- Base URL: `restFunctionHandle.location`
- Path: `/v1/function/<encoded-function-name>`
- Function name is URL-encoded for safety
- Example: `http://localhost:8080/v1/function/remote.default.strlen`

#### 3. REST Client (`RestRemoteClient.cpp/h`)

**Purpose**: Manages HTTP communication with remote function servers.

**Features**:
- **HTTP Client**: Uses Facebook Proxygen for high-performance HTTP
- **Connection Pooling**: Reuses connections for efficiency
- **Event-Driven**: Uses Folly's EventBase for async I/O
- **Configurable Timeouts**: 
  - Request timeout: `exchange-request-timeout-ms` (default from config)
  - Connect timeout: `exchange-connect-timeout-ms` (default from config)
- **HTTPS Support**: Automatically detects scheme from URL
- **Memory Management**: Uses Velox memory pools for buffer allocation

**HTTP Protocol**:
```
POST /v1/function/remote.default.strlen HTTP/1.1
Host: localhost:8080
Content-Type: application/vnd.presto.page (or application/vnd.spark.unsaferow)
Content-Length: <size>

<serialized function input data>

Response:
HTTP/1.1 200 OK
Content-Type: application/vnd.presto.page
Content-Length: <size>

<serialized function output data>
```

**Error Handling**:
- Validates HTTP status codes (must be 2xx)
- Throws `VeloxException` on errors with descriptive messages
- Handles connection failures and timeouts

#### 4. Remote Function Implementation (`RestRemoteFunction.cpp/h`)

**Purpose**: Velox-compatible function that wraps REST invocation.

**Inheritance**: `RestRemoteFunction` extends `velox::functions::RemoteVectorFunction`

**Key Method**:
```cpp
std::unique_ptr<RemoteFunctionResponse> invokeRemoteFunction(
    const RemoteFunctionRequest& request) const override {
    
    // Clone request payload
    auto requestBody = request.inputs()->payload()->clone();
    
    // Invoke REST endpoint
    auto responseBody = restClient_->invokeFunction(
        location_, serdeFormat_, std::move(requestBody));
    
    // Wrap response
    auto response = std::make_unique<RemoteFunctionResponse>();
    RemoteFunctionPage result;
    result.payload_ref() = std::move(*responseBody);
    response->result_ref() = std::move(result);
    
    return response;
}
```

**Metadata**:
```cpp
struct VeloxRemoteFunctionMetadata 
    : public velox::functions::RemoteVectorFunctionMetadata {
    std::string location;  // Full URL to function endpoint
};
```

### Data Flow

#### Function Call Flow

1. **Query Planning**: Presto coordinator identifies remote function and creates `RestFunctionHandle`
2. **Expression Conversion**: Native worker converts Presto expression to Velox expression
3. **Registration**: `registerRestRemoteFunction()` called (idempotent, thread-safe)
4. **Execution**: Velox evaluates expression, calls `RestRemoteFunction`
5. **Serialization**: Input vectors serialized to Presto Page or Spark UnsafeRow format
6. **HTTP Request**: `RestRemoteClient` sends POST to remote server
7. **Remote Execution**: Server processes request and returns result
8. **Deserialization**: Response deserialized back to Velox vectors
9. **Result**: Output vectors returned to query engine

#### Serialization Formats

**Presto Page Format** (`application/vnd.presto.page`):
- Native Presto serialization format
- Efficient for complex types
- Default format for Presto-to-Presto communication

**Spark UnsafeRow Format** (`application/vnd.spark.unsaferow`):
- Binary format compatible with Apache Spark
- Enables interoperability with Spark function servers
- Configured via `remote-function-server-serde` property

## Configuration

### System Properties

```properties
# Serialization format for remote function communication
# Options: "presto_page" (default), "spark_unsafe_row"
remote-function-server-serde=presto_page

# HTTP request timeout in milliseconds (inherited from exchange config)
exchange-request-timeout-ms=60000

# HTTP connection timeout in milliseconds (inherited from exchange config)
exchange-connect-timeout-ms=10000
```

### Build Configuration

Remote functions can be disabled at build time:
```cmake
# CMakeLists.txt
option(PRESTO_ENABLE_REMOTE_FUNCTIONS "Enable remote function support" ON)
```

When disabled, the code is excluded via preprocessor directives:
```cpp
#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
// Remote function code
#endif
```

## Testing Infrastructure

### Unit Tests (`RestFunctionHandleTest.cpp`)

**Purpose**: Validate registration and metadata handling

**Test Coverage**:
- Function signature parsing and conversion
- URL encoding and construction
- Thread-safe concurrent registration
- Duplicate registration handling
- Client caching and reuse

### C++ Integration Tests (`RemoteFunctionRestTest.cpp`)

**Purpose**: Test end-to-end remote function execution with mock server

**Components**:
- **Mock REST Server**: Embedded HTTP server for testing
- **Function Handlers**: Example implementations (strlen, fibonacci, etc.)
- **Test Fixtures**: Setup/teardown for server lifecycle

**Example Test**:
```cpp
TEST_F(RemoteFunctionRestTest, strlen) {
    auto result = evaluateOnce<int32_t>(
        "remote.default.strlen(c0)", 
        makeRowVector({makeFlatVector<std::string>({"hello", "world"})}));
    
    assertEqualVectors(
        makeFlatVector<int32_t>({5, 5}), 
        result);
}
```

### Server Test Infrastructure

#### `RemoteFunctionRestService` 
- HTTP server implementation using Proxygen
- Routes requests to registered function handlers
- Handles serialization/deserialization
- Thread-safe function registry

#### `RemoteFunctionRestHandler`
- Abstract base class for function implementations
- Defines interface: `getInputTypes()`, `getOutputType()`, `compute()`
- Example handlers in `tests/server/examples/`:
  - `RemoteStrLenHandler`: String length calculation
  - `RemoteFibonacciHandler`: Fibonacci sequence
  - `RemoteInverseCdfHandler`: Statistical function
  - `RemoteDoubleDivHandler`: Arithmetic operation
  - `RemoveCharHandler`: String manipulation

#### `RestFunctionRegistry`
- Manages function handler registration
- Maps function names to handler instances
- Thread-safe handler lookup

### Container-Based E2E Tests (`TestPrestoContainerRemoteFunction.java`)

**Purpose**: Validate complete system integration with Docker containers

**Architecture**:
- Presto coordinator container
- Native worker container
- Function server container (Java-based)
- All connected via Docker network

**Test Flow**:
1. Spin up containers with proper configuration
2. Execute SQL queries with remote function calls
3. Verify results match expected values

**Example Test**:
```java
@Test
public void testRemoteBasicTests() {
    assertEquals(
        computeActual("select remote.default.abs(-10)")
            .getMaterializedRows().get(0).getField(0).toString(),
        "10");
}
```

### Docker Configuration

**Dockerfile Changes**:
- Added `presto-function-server-executable.jar` to container
- Configured as `/opt/presto-remote-function-server`
- Exposed on configurable port (default: 8081)

**Container Network**:
- Workers configured with function server URL
- Example: `http://function-server:8081`

## Thread Safety

### Critical Sections

1. **Function Registration**:
   - Protected by `static std::mutex`
   - Double-checked locking pattern
   - Prevents duplicate registration race conditions

2. **Client Cache**:
   - `std::unordered_map<std::string, RestRemoteClientPtr>` protected by same mutex
   - Ensures one client instance per location URL

3. **Test Server Registry**:
   - Function handler map protected by mutex
   - Safe concurrent handler registration/unregistration

### Lock-Free Operations

- HTTP requests (lock-free once client is created)
- Function execution (thread-safe by design)
- Serialization/deserialization (independent per request)

## Performance Considerations

### Optimization Strategies

1. **Lazy Initialization**: Clients created only when first needed
2. **Client Reuse**: One client per location, shared across functions
3. **Connection Pooling**: HTTP client maintains persistent connections
4. **One-Time Registration**: Functions registered once per location
5. **No Registration Locks During Execution**: Registration lock released before function calls

### Performance Characteristics

- **Registration Overhead**: O(1) after first registration (cache hit)
- **Network Latency**: Dominant factor, depends on server distance
- **Serialization**: O(n) where n = input data size
- **Memory**: Bounded by input/output vector sizes

### Scalability

- **Concurrent Requests**: Multiple threads can execute remote functions simultaneously
- **Client Pooling**: Proxygen handles concurrent requests per client
- **Server Scalability**: External concern, can be load-balanced

## Error Handling

### Client-Side Errors

1. **Connection Failures**:
   - Timeout after `connect-timeout-ms`
   - Throws `VeloxException` with error details

2. **HTTP Errors**:
   - Non-2xx status codes result in exception
   - Error message includes status code and response body

3. **Serialization Errors**:
   - Invalid data format throws during deserialization
   - Type mismatches caught by Velox type system

### Server-Side Errors

Function servers should:
- Return 200 OK for successful execution
- Return 4xx for client errors (invalid input)
- Return 5xx for server errors (execution failure)
- Include error details in response body

## Future Enhancements

### Potential Improvements

1. **Authentication/Authorization**:
   - Add support for API keys, OAuth tokens
   - Integrate with Presto security model

2. **Service Discovery**:
   - Automatic discovery of function servers
   - Integration with service mesh (e.g., Istio)

3. **Load Balancing**:
   - Client-side load balancing across multiple servers
   - Health checks and failover

4. **Caching**:
   - Cache deterministic function results
   - Reduce network round trips

5. **Metrics and Monitoring**:
   - Request latency tracking
   - Error rate monitoring
   - Integration with Presto metrics system

6. **Batch Optimization**:
   - Batch multiple function calls in single HTTP request
   - Reduce overhead for small data batches

7. **Circuit Breaker**:
   - Fail fast when remote server is down
   - Prevent cascading failures

## Dependencies

### External Libraries

- **Proxygen**: HTTP client/server framework
- **Folly**: Facebook's C++ utilities library
- **Boost.URL**: URL encoding/parsing
- **Velox**: Vector processing engine

### Presto Components

- **presto_protocol**: Protocol buffer definitions
- **presto_cpp/main/http**: HTTP client infrastructure
- **presto_cpp/main/common**: Configuration management

## Security Considerations

### Current Implementation

- No built-in authentication
- Supports HTTPS via URL scheme
- URL encoding prevents injection attacks
- Input validation via Velox type system

### Best Practices

1. **Use HTTPS**: Always use encrypted connections in production
2. **Network Isolation**: Deploy function servers in trusted network
3. **Input Validation**: Server should validate all inputs
4. **Rate Limiting**: Implement at server or proxy layer
5. **Audit Logging**: Log all remote function invocations

## Migration Guide

### Enabling Remote Functions

1. **Build with support**:
   ```bash
   cmake -DPRESTO_ENABLE_REMOTE_FUNCTIONS=ON ..
   make
   ```

2. **Configure workers**:
   ```properties
   remote-function-server-serde=presto_page
   ```

3. **Deploy function server**:
   - Implement function handlers
   - Deploy HTTP server
   - Ensure network connectivity from workers

4. **Register functions**:
   - Use Presto catalog configuration
   - Define function signatures and locations

### Adding New Remote Functions

1. **Server Side**:
   - Implement function logic
   - Expose HTTP endpoint: `/v1/function/<name>`
   - Handle serialization format

2. **Client Side**:
   - Register function in Presto catalog
   - Define signature and location
   - Workers automatically handle execution

## Appendix

### File Structure

```
presto-native-execution/
├── presto_cpp/main/functions/remote/
│   ├── CMakeLists.txt
│   ├── PrestoRestFunctionRegistration.{cpp,h}  # Registration logic
│   ├── RestRemoteFunction.{cpp,h}              # Velox function wrapper
│   ├── client/
│   │   ├── CMakeLists.txt
│   │   └── RestRemoteClient.{cpp,h}            # HTTP client
│   ├── tests/
│   │   ├── CMakeLists.txt
│   │   ├── RemoteFunctionRestTest.cpp          # C++ integration tests
│   │   └── server/                             # Test server infrastructure
│   │       ├── RemoteFunctionRestService.{cpp,h}
│   │       ├── RemoteFunctionRestHandler.h
│   │       ├── RestFunctionRegistry.{cpp,h}
│   │       └── examples/                       # Example function handlers
│   │           ├── RemoteStrLenHandler.h
│   │           ├── RemoteFibonacciHandler.h
│   │           └── ...
│   └── utils/
│       └── ContentTypes.h                      # HTTP content types
├── presto_cpp/main/types/
│   ├── PrestoToVeloxExpr.cpp                   # Expression integration
│   └── tests/
│       └── RestFunctionHandleTest.cpp          # Unit tests
└── src/test/java/.../nativeworker/
    ├── ContainerQueryRunner.java               # Docker test framework
    └── TestPrestoContainerRemoteFunction.java  # E2E tests
```

### Key Classes and Functions

| Component | Purpose |
|-----------|---------|
| `RestFunctionHandle` | Presto protocol type for remote function metadata |
| `registerRestRemoteFunction()` | Main registration entry point |
| `RestRemoteClient` | HTTP client for remote invocations |
| `RestRemoteFunction` | Velox function implementation |
| `VeloxRemoteFunctionMetadata` | Function metadata (location, format) |
| `RemoteFunctionRestService` | Test HTTP server |
| `RemoteFunctionRestHandler` | Abstract base for function implementations |

### Protocol Reference

**Function ID Format**: `namespace.schema.function;TYPE1;TYPE2;...`
- Example: `remote.default.strlen;varchar`

**URL Format**: `{location}/v1/function/{encoded-function-name}`
- Example: `http://localhost:8080/v1/function/remote.default.strlen`

**Content Types**:
- Presto Page: `application/vnd.presto.page`
- Spark UnsafeRow: `application/vnd.spark.unsaferow`

---

**Document Version**: 1.0  
**Last Updated**: 2024-11-13  
**Authors**: Presto Remote Functions Team

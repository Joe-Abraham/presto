# Remote Function Execution - Reviewer Guide

## Purpose of This Document

This guide helps reviewers efficiently understand and review the remote function execution feature. It provides a structured approach to reviewing the changes, highlighting key areas of focus and dependencies between components.

## Quick Overview

**What does this PR do?**
- Adds ability for Presto native workers to execute functions on remote HTTP/REST servers
- Enables extensibility without modifying the core engine
- Provides thread-safe registration and execution infrastructure

**Why is this needed?**
- Allows custom functions to be hosted externally
- Reduces need to rebuild/redeploy workers for new functions
- Enables function implementations in different languages/frameworks

**Impact:**
- New feature, no breaking changes
- Optional (can be disabled at build time with `-DPRESTO_ENABLE_REMOTE_FUNCTIONS=OFF`)
- Zero impact when feature is not used

## Review Strategy

### Suggested Review Order

We recommend reviewing in this order to build understanding progressively:

1. **Configuration & Build System** (5-10 min)
2. **Core Data Structures** (10 min)
3. **REST Client** (15 min)
4. **Registration Logic** (20 min)
5. **Expression Integration** (10 min)
6. **Testing Infrastructure** (30 min)
7. **End-to-End Tests** (15 min)

**Total estimated review time:** ~2 hours

---

## 1. Configuration & Build System

### Files to Review
- `presto-native-execution/presto_cpp/main/common/Configs.h` (lines with `remoteFunctionServerSerde`)
- `presto-native-execution/presto_cpp/main/common/Configs.cpp` (implementation)
- `presto-native-execution/CMakeLists.txt` (build flag)

### What to Look For
- ✅ Configuration property name follows conventions
- ✅ Default values are sensible
- ✅ Build flag properly isolates feature with `#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS`

### Key Points
- Config: `remote-function-server-serde` supports `"presto_page"` (default) or `"spark_unsafe_row"`
- Feature can be completely disabled at compile time
- Uses existing exchange timeout configs for HTTP client

---

## 2. Core Data Structures

### Files to Review
- `presto-native-execution/presto_cpp/main/functions/remote/RestRemoteFunction.h`
- `presto-native-execution/presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.h`

### What to Look For
- ✅ Clean separation of concerns
- ✅ Appropriate use of smart pointers
- ✅ Clear struct/class responsibilities

### Key Components

**`VeloxRemoteFunctionMetadata`**
```cpp
struct VeloxRemoteFunctionMetadata 
    : public velox::functions::RemoteVectorFunctionMetadata {
    std::string location;  // HTTP endpoint URL
};
```
- Simple metadata holder
- Extends Velox's base metadata class

**`RestRemoteClient`**
- Wraps HTTP client (Proxygen)
- Thread-safe by design (one instance per location)
- Immutable after construction

---

## 3. REST Client Implementation

### Files to Review
- `presto-native-execution/presto_cpp/main/functions/remote/client/RestRemoteClient.h`
- `presto-native-execution/presto_cpp/main/functions/remote/client/RestRemoteClient.cpp`

### What to Look For
- ✅ **Error handling**: HTTP status codes validated (must be 2xx)
- ✅ **Resource management**: Proper cleanup in destructor
- ✅ **Thread safety**: EventBase used correctly (events run in dedicated thread)
- ✅ **Timeouts**: Both connect and request timeouts configured

### Critical Review Points

**Constructor** (`RestRemoteClient.cpp:35-51`):
```cpp
RestRemoteClient::RestRemoteClient(const std::string& url) : url_(url) {
    memPool_ = memory::MemoryManager::getInstance()->addLeafPool();
    folly::Uri uri(url_);
    // ... creates HTTP client with proper endpoint
}
```
- Parses URL to extract host, port, scheme
- Creates event base thread (single thread per client)
- Initializes HTTP client with timeouts from config

**invokeFunction** (`RestRemoteClient.cpp:62-113`):
```cpp
std::unique_ptr<folly::IOBuf> invokeFunction(
    const std::string& fullUrl,
    velox::functions::remote::PageFormat serdeFormat,
    std::unique_ptr<folly::IOBuf> requestPayload) const
```
- Sends POST request with appropriate Content-Type header
- Validates HTTP status (200-299)
- Returns response body as IOBuf
- Throws VeloxException on errors with descriptive messages

**Error Handling**:
- Connection timeouts → exception with timeout info
- Non-2xx status → exception with status code and body
- Missing response → exception

---

## 4. Registration Logic (Most Complex Part)

### Files to Review
- `presto-native-execution/presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.cpp`
- `presto-native-execution/presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.h`

### What to Look For
- ✅ **Thread safety**: Static variables protected by mutex
- ✅ **Idempotency**: Duplicate registration handled correctly
- ✅ **Resource efficiency**: Client reuse per location
- ✅ **URL construction**: Proper encoding of function names

### Critical Review Points

**Thread Safety** (`PrestoRestFunctionRegistration.cpp:114-122`):
```cpp
void registerRestRemoteFunction(
    const protocol::RestFunctionHandle& restFunctionHandle) {
    
    static std::mutex mutex;
    static std::unordered_set<std::string> registeredFunctionHandles;
    static std::unordered_map<std::string, RestRemoteClientPtr> remoteClients;
    
    std::lock_guard<std::mutex> lock(mutex);
    // ... registration logic
}
```
- ⚠️ **CRITICAL**: All static variables protected by single mutex
- Registration is idempotent (checks `registeredFunctionHandles` before registering)
- Client instances cached per location URL (prevents duplicate connections)

**URL Construction** (`PrestoRestFunctionRegistration.cpp:130-145`):
```cpp
const auto fullUrl = fmt::format(
    "{}/v1/function/{}", 
    restFunctionHandle.location,
    urlEncode(functionName));
```
- Function name extracted from `SqlFunctionId` (format: `namespace.schema.name;TYPE;TYPE`)
- URL-encoded for safety
- Path format: `/v1/function/<encoded-name>`

**Signature Conversion** (`PrestoRestFunctionRegistration.cpp:73-111`):
- Converts Presto `Signature` → Velox `FunctionSignaturePtr`
- Handles type variables, constraints, variadic arguments
- Preserves all signature metadata

---

## 5. Expression Integration

### Files to Review
- `presto-native-execution/presto_cpp/main/types/PrestoToVeloxExpr.cpp` (lines ~522-534)

### What to Look For
- ✅ Minimal invasiveness (only adds one conditional branch)
- ✅ Guarded by `#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS`
- ✅ Follows existing pattern for other function handle types

### Key Change
```cpp
#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
else if (auto restFunctionHandle = 
         std::dynamic_pointer_cast<protocol::RestFunctionHandle>(
             pexpr.functionHandle)) {
    auto args = toVeloxExpr(pexpr.arguments);
    auto returnType = typeParser_->parse(pexpr.returnType);
    
    // Register function (idempotent, thread-safe)
    functions::remote::rest::registerRestRemoteFunction(*restFunctionHandle);
    
    // Create standard CallTypedExpr
    return std::make_shared<CallTypedExpr>(
        returnType, args, getFunctionName(restFunctionHandle->functionId));
}
#endif
```

**Why registration happens here:**
- Triggered during query planning/expression conversion
- Ensures function is registered before execution
- Idempotent design means safe to call multiple times
- Lock released before function execution

---

## 6. Testing Infrastructure

### Files to Review (in order)

#### 6.1 Test Server Base Classes
- `presto-native-execution/presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestHandler.h`
  - Abstract base class for function implementations
  - Defines interface: `getInputTypes()`, `getOutputType()`, `compute()`

- `presto-native-execution/presto_cpp/main/functions/remote/tests/server/RestFunctionRegistry.{h,cpp}`
  - Thread-safe registry for function handlers
  - Maps function names to handler instances

- `presto-native-execution/presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestService.{h,cpp}`
  - HTTP server implementation using Proxygen
  - Routes `/v1/function/<name>` to appropriate handler
  - Handles serialization/deserialization

#### 6.2 Example Function Handlers
- `presto-native-execution/presto_cpp/main/functions/remote/tests/server/examples/`
  - `RemoteStrLenHandler.h`: String length (simple example)
  - `RemoteFibonacciHandler.h`: Fibonacci calculation (compute-intensive)
  - `RemoteInverseCdfHandler.h`: Statistical function (complex types)
  - `RemoteDoubleDivHandler.h`: Division operation (error handling)
  - `RemoveCharHandler.h`: String manipulation (multiple inputs)

### What to Look For
- ✅ Clean handler interface (easy to implement new functions)
- ✅ Thread safety in registry
- ✅ Proper serialization format handling
- ✅ Example handlers cover various scenarios (simple, complex, errors)

### Key Points
- Test server is **only for testing** (not production code)
- Demonstrates how external function servers should be implemented
- Handler interface is simple and focused

---

## 7. Tests

### Unit Tests
**File**: `presto-native-execution/presto_cpp/main/types/tests/RestFunctionHandleTest.cpp`

**Coverage**:
- Function ID parsing
- Signature conversion (Presto → Velox)
- URL construction and encoding
- Thread-safe concurrent registration
- Client caching and reuse

**What to Look For**:
- ✅ Tests cover edge cases (special characters in names, concurrent access)
- ✅ Validates idempotent registration
- ✅ Confirms client instances are reused

### Integration Tests
**File**: `presto-native-execution/presto_cpp/main/functions/remote/tests/RemoteFunctionRestTest.cpp`

**Coverage**:
- End-to-end function execution with mock server
- Various data types (integers, strings, complex types)
- Null handling
- Error scenarios

**What to Look For**:
- ✅ Server lifecycle managed properly (setup/teardown)
- ✅ Tests verify actual computation results
- ✅ Error cases handled appropriately

### E2E Container Tests
**File**: `presto-native-execution/src/test/java/com/facebook/presto/nativeworker/TestPrestoContainerRemoteFunction.java`

**Coverage**:
- Full system integration (coordinator + worker + function server)
- Real SQL queries with remote functions
- Docker container orchestration

**What to Look For**:
- ✅ Container setup mimics production deployment
- ✅ Tests use actual SQL queries (user-facing interface)
- ✅ Verifies coordinator-to-worker-to-function-server flow

---

## Security Considerations

### Current Implementation
- ✅ **URL encoding**: Function names properly encoded (prevents injection)
- ✅ **HTTPS support**: Scheme detected from URL, TLS handled by Proxygen
- ⚠️ **No authentication**: Currently no auth mechanism (acceptable for v1)
- ⚠️ **Input validation**: Relies on Velox type system (acceptable)

### Recommendations for Reviewers
- Consider if auth should be added before merge (suggest: follow-up PR)
- Verify URL encoding is comprehensive
- Check if additional input validation is needed for your use case

---

## Performance Considerations

### Optimization Strategies
1. **Lazy initialization**: Clients created only when needed
2. **Client reuse**: One client per location URL (not per function)
3. **Connection pooling**: Proxygen maintains persistent connections
4. **One-time registration**: Registration overhead amortized across all calls
5. **Lock-free execution**: Registration lock released before function calls

### Performance Characteristics
- **Registration**: O(1) after first call (hash map lookup)
- **Network latency**: Dominant factor (depends on server distance)
- **Serialization**: O(n) where n = data size (standard Velox serialization)

### Potential Bottlenecks
- ⚠️ **Global registration lock**: All threads contend on single mutex during registration
  - Mitigation: Registration happens once per function, not per call
  - Consider: Lock-free concurrent hash map in follow-up if needed

- ⚠️ **Network I/O**: Remote calls add latency
  - Mitigation: Connection pooling reduces overhead
  - Consider: Batching multiple calls in follow-up

---

## Common Review Questions

### Q: Why register during expression conversion instead of startup?
**A**: Functions are discovered dynamically from queries. We don't know which remote functions will be used until query planning. Registration is idempotent and thread-safe, so repeated calls are cheap.

### Q: Is the global registration lock a bottleneck?
**A**: No. Registration happens once per function (cached). Lock is only held during registration, not during function execution. In steady state, lock contention is negligible.

### Q: What happens if the remote server is down?
**A**: HTTP client throws exception after timeout. Exception propagates to query, which fails. No circuit breaker or retry logic in v1 (potential follow-up).

### Q: Can this be used in production?
**A**: Yes, with caveats:
- Deploy function servers in trusted network (no auth in v1)
- Use HTTPS for sensitive data
- Monitor remote server health externally
- Consider adding circuit breaker in follow-up

### Q: Why not use gRPC instead of REST?
**A**: REST chosen for:
- Simplicity and broad compatibility
- Existing HTTP infrastructure in Presto
- Easy debugging with standard tools
- Can add gRPC in follow-up if needed

---

## Files Changed Summary

### Core Implementation (C++)
```
presto-native-execution/presto_cpp/main/
├── functions/remote/
│   ├── PrestoRestFunctionRegistration.{cpp,h}  [173 + 23 lines]   ← Registration
│   ├── RestRemoteFunction.{cpp,h}              [103 + 35 lines]   ← Velox function
│   ├── client/RestRemoteClient.{cpp,h}         [113 + 53 lines]   ← HTTP client
│   └── utils/ContentTypes.h                    [22 lines]         ← Constants
├── types/PrestoToVeloxExpr.cpp                 [+18 lines]        ← Integration
└── common/Configs.{cpp,h}                      [+10 lines]        ← Config
```

### Testing Infrastructure (C++)
```
presto-native-execution/presto_cpp/main/functions/remote/tests/
├── RemoteFunctionRestTest.cpp                  [299 lines]        ← Integration tests
├── server/
│   ├── RemoteFunctionRestService.{cpp,h}       [312 + 125 lines]  ← Test server
│   ├── RemoteFunctionRestHandler.h             [77 lines]         ← Handler interface
│   ├── RestFunctionRegistry.{cpp,h}            [69 + 77 lines]    ← Registry
│   └── examples/                               [~60 lines each]   ← Example handlers
│       ├── RemoteStrLenHandler.h
│       ├── RemoteFibonacciHandler.h
│       └── ... (5 handlers total)
└── types/tests/RestFunctionHandleTest.cpp      [451 lines]        ← Unit tests
```

### E2E Tests (Java)
```
presto-native-execution/src/test/java/.../nativeworker/
├── TestPrestoContainerRemoteFunction.java      [84 lines]         ← E2E tests
├── ContainerQueryRunner.java                   [+45 lines]        ← Test framework
└── ContainerQueryRunnerUtils.java              [+55 lines]        ← Utilities
```

### Build & Docker
```
presto-native-execution/
├── CMakeLists.txt                              [+2 lines]         ← Build flag
├── pom.xml                                     [+8 lines]         ← Maven deps
└── docker/Dockerfile                           [+2 lines]         ← Function server
```

**Total**: ~2,600 lines added across 45 files

---

## Red Flags to Watch For

### During Review, Be Alert For:

❌ **Thread Safety Issues**
- [ ] Static variables without mutex protection
- [ ] Shared mutable state accessed concurrently
- [ ] Race conditions in client creation

✅ **Current Implementation**: All static variables protected by mutex

❌ **Resource Leaks**
- [ ] HTTP clients not properly cleaned up
- [ ] Memory pools not released
- [ ] EventBase threads not joined

✅ **Current Implementation**: Destructor properly cleans up resources

❌ **Security Vulnerabilities**
- [ ] SQL injection via function names
- [ ] Buffer overflows in serialization
- [ ] Unvalidated user input

✅ **Current Implementation**: Function names URL-encoded, Velox handles serialization safely

❌ **Breaking Changes**
- [ ] Changes to existing APIs
- [ ] Modifications to protocol definitions
- [ ] Altered function semantics

✅ **Current Implementation**: Pure addition, no breaking changes

---

## Approval Checklist

Before approving, ensure:

- [ ] **Build System**: Feature can be disabled with `-DPRESTO_ENABLE_REMOTE_FUNCTIONS=OFF`
- [ ] **Thread Safety**: Registration logic is thread-safe (mutex protects static vars)
- [ ] **Resource Management**: HTTP clients and threads properly cleaned up
- [ ] **Error Handling**: HTTP errors result in clear exceptions
- [ ] **Testing**: Unit tests, integration tests, and E2E tests all pass
- [ ] **Documentation**: Design doc and reviewer guide are clear
- [ ] **Code Style**: Follows Presto C++ style guidelines
- [ ] **No Breaking Changes**: Existing functionality unaffected

---

## Questions for the Author

If anything is unclear, consider asking:

1. **Performance**: Have you benchmarked the registration overhead with concurrent threads?
2. **Error Handling**: What happens if a remote server returns malformed data?
3. **Monitoring**: How would operators monitor remote function health in production?
4. **Future Work**: Are there plans for auth, circuit breakers, or retries?

---

## Additional Resources

- **Detailed Design Doc**: `REMOTE_FUNCTIONS_DESIGN.md` - comprehensive technical design
- **Protocol Spec**: See design doc "Protocol Reference" section for wire format
- **Example Usage**: See `TestPrestoContainerRemoteFunction.java` for SQL examples

---

**Document Version**: 1.0  
**Last Updated**: 2024-11-13  
**Estimated Review Time**: 2 hours  
**Complexity**: Medium-High

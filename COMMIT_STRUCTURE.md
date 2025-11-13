# Rebased Commit Structure

This PR has been rebased into 5 logical, reviewable commits following conventional commit standards.

## How to Apply This Rebase

To apply this rebased structure to the PR, run:

```bash
# First, ensure you have the rebased commits locally
git fetch origin copilot/sub-pr-14
git checkout copilot/sub-pr-14

# Reset to the rebased commit structure
git reset --hard 8c90525

# Force push the rebased history
git push --force origin copilot/sub-pr-14
```

**Warning**: This rewrites commit history. Ensure no one else has pulled the old commits.

---

## Commit Structure Overview

## Commit 1: feat(functions): add REST client infrastructure for remote function calls (8ba19bb)

**Purpose**: Foundation for HTTP communication with remote function servers

**Files**:
- `presto_cpp/main/functions/remote/client/RestRemoteClient.{h,cpp}` - HTTP client implementation
- `presto_cpp/main/functions/remote/client/CMakeLists.txt` - Build configuration
- `presto_cpp/main/functions/remote/utils/ContentTypes.h` - HTTP content type utilities
- `presto_cpp/main/http/HttpConstants.h` - HTTP constant extensions

**Changes**: 5 files, +203 lines

---

## Commit 2: feat(functions): implement remote function registration and execution (fb5b9bf)

**Purpose**: Core remote function lifecycle management with thread-safe registration

**Files**:
- `presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.{h,cpp}` - Function registration
- `presto_cpp/main/functions/remote/RestRemoteFunction.{h,cpp}` - Remote function wrapper
- `presto_cpp/main/functions/remote/CMakeLists.txt` - Module build config
- `presto_cpp/main/functions/CMakeLists.txt` - Integration
- `presto_cpp/main/common/Configs.{h,cpp}` - Configuration support
- `presto_cpp/main/common/Utils.{h,cpp}` - Utility functions

**Changes**: 10 files, +394 lines, -1 line

---

## Commit 3: feat(expr): integrate remote functions into expression evaluation system (2a709fc)

**Purpose**: Wire remote functions into Presto's expression compilation pipeline

**Files**:
- `presto_cpp/main/types/PrestoToVeloxExpr.{h,cpp}` - Expression conversion
- `presto_cpp/main/types/CMakeLists.txt` - Linking
- `presto_cpp/main/functions/FunctionMetadata.cpp` - Metadata support
- `presto_cpp/main/PrestoServer.cpp` - Initialization
- `presto-native-execution/CMakeLists.txt` - Top-level build

**Changes**: 6 files, +29 lines, -21 lines

---

## Commit 4: test(functions): add mock REST server and comprehensive unit tests (51b6e99)

**Purpose**: Complete C++ test infrastructure for remote functions

**Files**:
- `presto_cpp/main/functions/remote/tests/RemoteFunctionRestTest.cpp` - Unit tests
- `presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestService.{h,cpp}` - Mock server
- `presto_cpp/main/functions/remote/tests/server/RestFunctionRegistry.{h,cpp}` - Handler registry
- `presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestHandler.h` - Handler interface
- `presto_cpp/main/functions/remote/tests/server/examples/*.h` - Example handlers (5 files)
- `presto_cpp/main/types/tests/RestFunctionHandleTest.cpp` - Protocol tests
- CMakeLists.txt files for test configuration

**Changes**: 15 files, +1787 lines, -15 lines

---

## Commit 5: test(e2e): add container-based E2E tests for remote functions (8c90525)

**Purpose**: End-to-end validation in containerized environment

**Files**:
- `src/test/java/.../nativeworker/TestPrestoContainerRemoteFunction.java` - E2E tests
- `src/test/java/.../nativeworker/ContainerQueryRunner.java` - Enhanced runner
- `src/test/java/.../nativeworker/ContainerQueryRunnerUtils.java` - Test utilities
- `docker/Dockerfile` - Container configuration
- `presto-native-execution/.dockerignore` - Docker ignore rules
- `presto-native-execution/pom.xml` - Maven dependencies
- `.gitignore` - Artifact exclusions

**Changes**: 7 files, +245 lines, -41 lines

---

## Benefits of This Structure

1. **Incremental Review**: Each commit can be reviewed independently
2. **Logical Progression**: Follows natural development flow (client → registration → integration → tests)
3. **Clear Scope**: Each commit has a focused, well-defined purpose
4. **Conventional Commits**: Follows standard format (feat/test prefixes)
5. **Bisectable**: Each commit represents a complete, logical unit of work

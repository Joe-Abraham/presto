/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.h"
#include "presto_cpp/main/functions/remote/RestRemoteFunction.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PortUtil.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
namespace facebook::presto::functions::remote::rest::test {
namespace {

class PrestoRestFunctionRegistrationTest
    : public velox::functions::test::FunctionBaseTest {
 public:
  static constexpr const char* kFunctionSchema = "remote.schema";
  static constexpr const char* kFunctionIdTypeSuffix = ";integer";
  
  void SetUp() override {
    // Initialize system config with a default REST URL
    SystemConfig::instance()->setValue(
        std::string(SystemConfig::kRemoteFunctionServerRestURL),
        "http://default-server:8080");
    SystemConfig::instance()->setValue(
        std::string(SystemConfig::kRemoteFunctionServerSerde), "presto_page");
  }

  protocol::RestFunctionHandle createTestFunctionHandle(
      const std::string& functionName,
      const std::optional<std::string>& executionEndpoint = std::nullopt) {
    protocol::RestFunctionHandle handle;
    handle.functionId = std::string(kFunctionSchema) + "." + functionName + kFunctionIdTypeSuffix;
    handle.version = "1";
    
    protocol::Signature signature;
    signature.name = std::string(kFunctionSchema) + "." + functionName;
    signature.kind = protocol::FunctionKind::SCALAR;
    signature.returnType = "integer";
    signature.argumentTypes = {"integer"};
    signature.variableArity = false;
    handle.signature = signature;
    
    if (executionEndpoint.has_value()) {
      handle.executionEndpoint = std::make_shared<std::string>(executionEndpoint.value());
    }
    
    return handle;
  }
  
  void verifyFunctionIsRegistered(const std::string& functionName) {
    EXPECT_TRUE(exec::getVectorFunctionSignatures(functionName) != std::nullopt);
  }
};

// Test that registering a function without executionEndpoint uses the default URL
TEST_F(PrestoRestFunctionRegistrationTest, registerWithoutExecutionEndpoint) {
  auto handle = createTestFunctionHandle("test_default_endpoint");
  
  // This should succeed - the function will be registered with the default URL
  // from SystemConfig
  EXPECT_NO_THROW(registerRestRemoteFunction(handle));
  
  // Verify the function is registered
  verifyFunctionIsRegistered("test_default_endpoint");
}

// Test that registering a function with executionEndpoint uses the provided URL
// This test verifies that the executionEndpoint is properly fed through to
// VeloxRemoteFunctionMetadata.location by attempting to call a function
// with an invalid endpoint and checking the error message contains the endpoint
TEST_F(PrestoRestFunctionRegistrationTest, registerWithExecutionEndpoint) {
  const std::string customEndpoint = "http://custom-server:9999";
  auto handle = createTestFunctionHandle("test_custom_endpoint", customEndpoint);
  
  // Register the function with custom executionEndpoint
  EXPECT_NO_THROW(registerRestRemoteFunction(handle));
  
  // Verify the function is registered
  verifyFunctionIsRegistered("test_custom_endpoint");
  
  // Try to invoke the function - it should fail with a connection error
  // that includes the custom endpoint URL, proving that the executionEndpoint
  // was properly used to set VeloxRemoteFunctionMetadata.location
  auto inputVector = makeFlatVector<int32_t>({1, 2, 3});
  auto data = makeRowVector({inputVector});
  
  try {
    evaluate<SimpleVector<int32_t>>("test_custom_endpoint(c0)", data);
    FAIL() << "Expected function call to fail with connection error";
  } catch (const std::exception& e) {
    std::string errorMsg = e.what();
    // The error should contain the custom endpoint URL, proving it was used
    EXPECT_TRUE(errorMsg.find(customEndpoint) != std::string::npos)
        << "Error message should contain custom endpoint URL. Got: " << errorMsg;
  }
}

// Test that the same function can be registered multiple times with different
// endpoints and the latest registration wins
TEST_F(PrestoRestFunctionRegistrationTest, reregisterWithDifferentEndpoint) {
  const std::string firstEndpoint = "http://first-server:8080";
  const std::string secondEndpoint = "http://second-server:9090";
  
  // Register with first endpoint
  auto handle1 = createTestFunctionHandle("test_reregister", firstEndpoint);
  EXPECT_NO_THROW(registerRestRemoteFunction(handle1));
  
  // Re-register with second endpoint
  auto handle2 = createTestFunctionHandle("test_reregister", secondEndpoint);
  EXPECT_NO_THROW(registerRestRemoteFunction(handle2));
  
  // Verify the function is still registered
  verifyFunctionIsRegistered("test_reregister");
  
  // Try to invoke - the error should contain the second endpoint,
  // proving the re-registration updated the metadata.location
  auto inputVector = makeFlatVector<int32_t>({1});
  auto data = makeRowVector({inputVector});
  
  try {
    evaluate<SimpleVector<int32_t>>("test_reregister(c0)", data);
    FAIL() << "Expected function call to fail with connection error";
  } catch (const std::exception& e) {
    std::string errorMsg = e.what();
    EXPECT_TRUE(errorMsg.find(secondEndpoint) != std::string::npos)
        << "Error should contain second endpoint. Got: " << errorMsg;
  }
}

} // namespace
} // namespace facebook::presto::functions::remote::rest::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}

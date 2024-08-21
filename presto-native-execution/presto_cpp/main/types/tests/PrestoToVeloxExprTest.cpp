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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <array>

#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/core/Expressions.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/remote/client/Remote.h"
#include "velox/type/Type.h"

using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::functions;
using ::testing::_;
using ::testing::Return;

namespace facebook::velox::functions {

// Mock structure for RemoteVectorFunctionMetadata
struct MockRemoteVectorFunctionMetadata : public exec::VectorFunctionMetadata {
  MOCK_METHOD(void, setLocation, (const folly::SocketAddress&), ());
  MOCK_METHOD(folly::SocketAddress, getLocation, (), (const));

  MOCK_METHOD(void, setSerdeFormat, (remote::PageFormat), ());
  MOCK_METHOD(remote::PageFormat, getSerdeFormat, (), (const));

  // Constructor to initialize default mock behavior
  MockRemoteVectorFunctionMetadata() {
    ON_CALL(*this, getLocation())
        .WillByDefault(testing::Return(folly::SocketAddress()));
    ON_CALL(*this, getSerdeFormat())
        .WillByDefault(testing::Return(remote::PageFormat::PRESTO_PAGE));
  }
};

// Mock class for the function registry
class MockFunctionRegistry {
 public:
  MOCK_METHOD(
      void,
      registerRemoteFunction,
      (const std::string& name,
       const std::vector<exec::FunctionSignaturePtr>& signatures,
       const RemoteVectorFunctionMetadata& metadata,
       bool overwrite),
      ());
};

} // namespace facebook::velox::functions

class PrestoToVeloxExprTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::MemoryManager::getInstance()->addLeafPool();
    converter_ =
        std::make_unique<VeloxExprConverter>(pool_.get(), &typeParser_);
    mockFunctionRegistry = std::make_shared<MockFunctionRegistry>();
  }

  void testConstantExpression(
      const std::string& str,
      const std::string& type,
      const std::string& value) {
    json j = json::parse(str);
    std::shared_ptr<protocol::RowExpression> p = j;

    auto cexpr = std::static_pointer_cast<const ConstantTypedExpr>(
        converter_->toVeloxExpr(p));

    ASSERT_EQ(cexpr->type()->toString(), type);
    ASSERT_EQ(cexpr->value().toJson(cexpr->type()), value);
  }

  std::string makeCastToVarchar(
      bool isTryCast,
      const std::string& inputType,
      const std::string& returnType) {
    std::string signatureNameField = isTryCast
        ? R"("name": "presto.default.try_cast")"
        : R"("name": "presto.default.$operator$cast")";
    std::string inputTypeField = fmt::format("\"{}\"", inputType);
    std::string returnTypeField =
        fmt::format("\"returnType\": \"{}\"", returnType);

    std::string result = R"##(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "variable",
            "name": "my_col",
            "type": )##" +
        inputTypeField + R"##(
          }
        ],
        "displayName": "CAST",
        "functionHandle": {
          "@type": "$static",
          "signature": {
            "argumentTypes": [
    )##" +
        inputTypeField + R"##(
            ],
            "kind": "SCALAR",
    )##" +
        signatureNameField + R"##(,
            "longVariableConstraints": [],
    )##" +
        returnTypeField + R"##(,
            "typeVariableConstraints": [],
            "variableArity": false
          }
        },
    )##" +
        returnTypeField + R"##(
      }
    )##";

    return result;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VeloxExprConverter> converter_;
  TypeParser typeParser_;
  std::shared_ptr<MockFunctionRegistry> mockFunctionRegistry;
};

TEST_F(PrestoToVeloxExprTest, call) {
  static const std::array<std::string, 1> jsonStrings{
      R"##(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "variable",
            "name": "name",
            "type": "varchar(25)"
          },
          {
            "@type": "constant",
            "type": "varchar(25)",
            "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAMAAAAAAwAAAGZvbw=="
          }
        ],
        "displayName": "EQUAL",
        "functionHandle": {
          "@type": "rest",
          "functionId": "remote_function_id",
          "version": "1"
        },
        "returnType": "boolean"
      }
  )##"};

  static const std::array<std::string, 1> callExprNames{"remote_function_id"};

  for (size_t i = 0; i < jsonStrings.size(); ++i) {
    std::shared_ptr<protocol::RowExpression> p = json::parse(jsonStrings[i]);

    InputTypedExpr rowExpr(BIGINT());

    // Set expectations for the remote function
    if (callExprNames[i] == "remote_function_id") {
      EXPECT_CALL(*mockFunctionRegistry, registerRemoteFunction(_, _, _, _))
          .Times(1);
    }

    auto callexpr = std::static_pointer_cast<const CallTypedExpr>(
        converter_->toVeloxExpr(p));

    // Check some values ...
    ASSERT_EQ(callexpr->name(), callExprNames[i]);

    auto iexpr = callexpr->inputs();

    ASSERT_EQ(iexpr.size(), 2);

    {
      auto cexpr =
          std::static_pointer_cast<const FieldAccessTypedExpr>(iexpr[0]);
      ASSERT_EQ(cexpr->type()->toString(), "VARCHAR");
      ASSERT_EQ(cexpr->name(), "name");
    }
    {
      auto cexpr = std::static_pointer_cast<const ConstantTypedExpr>(iexpr[1]);
      ASSERT_EQ(cexpr->type()->toString(), "VARCHAR");
      ASSERT_EQ(cexpr->value().toJson(cexpr->type()), "\"foo\"");
    }
  }
}

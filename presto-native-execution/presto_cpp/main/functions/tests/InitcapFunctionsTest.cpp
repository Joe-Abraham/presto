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
#include "presto_cpp/main/functions/InitcapFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::presto::functions::test {

class InitcapFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  void SetUp() override {
    FunctionBaseTest::SetUp();
    registerInitcapFunctions("hive.default");
  }
};

TEST_F(InitcapFunctionsTest, basicTest) {
  // Test basic initcap functionality with strictspace=false
  auto result = evaluateOnce<std::string>("hive.default.initcap('hello world')", {});
  EXPECT_EQ("Hello World", result);
  
  // Test with various separators (strictspace=false)
  result = evaluateOnce<std::string>("hive.default.initcap('hello-world_test')", {});
  EXPECT_EQ("Hello-World_Test", result);
  
  // Test with numbers and special characters
  result = evaluateOnce<std::string>("hive.default.initcap('hello123world')", {});
  EXPECT_EQ("Hello123world", result);
  
  // Test with multiple spaces and punctuation
  result = evaluateOnce<std::string>("hive.default.initcap('hello,  world!test')", {});
  EXPECT_EQ("Hello,  World!Test", result);
  
  // Test empty string
  result = evaluateOnce<std::string>("hive.default.initcap('')", {});
  EXPECT_EQ("", result);
}

TEST_F(InitcapFunctionsTest, nullTest) {
  // Test with null input
  auto result = evaluateOnce<std::optional<std::string>>("hive.default.initcap(null)", {});
  EXPECT_EQ(std::nullopt, result);
}

} // namespace facebook::presto::functions::test
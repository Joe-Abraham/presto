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

#include <gtest/gtest.h>
#include "presto_cpp/main/functions/FunctionCatalogManager.h"
#include "presto_cpp/main/functions/dynamic_registry/CatalogFunctionRegistrar.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto::functions::test {

// Test function that uses catalog configuration
template <typename T>
struct TestCatalogFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    // Access catalog configuration
    auto* manager = FunctionCatalogManager::instance();
    auto config = manager->getCatalogConfig("test_catalog");

    std::string prefix = "default";
    if (config) {
      prefix = config->propertyOrDefault<std::string>("prefix", "default");
    }

    result = prefix + ":" + input;
  }
};

// Test function with session-aware configuration
template <typename T>
struct TestSessionAwareFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    auto* manager = FunctionCatalogManager::instance();
    
    // In a real scenario, session properties would be passed differently
    // For testing, we'll just check if the catalog exists
    auto config = manager->getCatalogConfig("test_catalog");
    
    if (config) {
      auto multiplier =
          config->propertyOrDefault<int>("multiplier", 1);
      std::string repeated;
      for (int i = 0; i < multiplier; i++) {
        if (i > 0) repeated += ",";
        repeated += input;
      }
      result = repeated;
    } else {
      result = input;
    }
  }
};

class FunctionCatalogIntegrationTest
    : public velox::functions::test::FunctionBaseTest {
 protected:
  void SetUp() override {
    velox::functions::test::FunctionBaseTest::SetUp();
    
    // Clear any existing catalogs
    FunctionCatalogManager::instance()->clear();

    // Register test catalog
    std::unordered_map<std::string, std::string> properties;
    properties["prefix"] = "TEST";
    properties["multiplier"] = "2";
    properties["enabled"] = "true";

    auto configBase =
        std::make_shared<const velox::config::ConfigBase>(std::move(properties));
    auto config = std::make_shared<FunctionCatalogConfig>(
        "test_catalog", configBase);

    FunctionCatalogManager::instance()->registerCatalog("test_catalog", config);
  }

  void TearDown() override {
    FunctionCatalogManager::instance()->clear();
    velox::functions::test::FunctionBaseTest::TearDown();
  }
};

TEST_F(FunctionCatalogIntegrationTest, RegisterAndUseCatalogFunction) {
  // Register function using catalog-based registration
  registerCatalogFunction<TestCatalogFunction, Varchar, Varchar>(
      "test_catalog", "default", "test_func");

  // Test the function
  auto result = evaluateOnce<std::string>(
      "\"test_catalog.default.test_func\"(c0)", std::optional<std::string>("hello"));

  EXPECT_EQ(result, "TEST:hello");
}

TEST_F(FunctionCatalogIntegrationTest, SessionAwareFunction) {
  // Register session-aware function
  registerCatalogFunction<TestSessionAwareFunction, Varchar, Varchar>(
      "test_catalog", "default", "repeat_func");

  // Test the function
  auto result = evaluateOnce<std::string>(
      "\"test_catalog.default.repeat_func\"(c0)", 
      std::optional<std::string>("word"));

  EXPECT_EQ(result, "word,word");
}

TEST_F(FunctionCatalogIntegrationTest, FunctionWithoutCatalog) {
  // Register function in non-existent catalog
  registerCatalogFunction<TestCatalogFunction, Varchar, Varchar>(
      "nonexistent", "default", "missing_func");

  // Function should still work but use defaults
  auto result = evaluateOnce<std::string>(
      "\"nonexistent.default.missing_func\"(c0)", 
      std::optional<std::string>("test"));

  EXPECT_EQ(result, "default:test");
}

TEST_F(FunctionCatalogIntegrationTest, CatalogConfigurationChanges) {
  // Register initial function
  registerCatalogFunction<TestCatalogFunction, Varchar, Varchar>(
      "test_catalog", "default", "dynamic_func");

  // Test with original config
  auto result1 = evaluateOnce<std::string>(
      "\"test_catalog.default.dynamic_func\"(c0)", 
      std::optional<std::string>("value"));
  EXPECT_EQ(result1, "TEST:value");

  // Update catalog configuration
  std::unordered_map<std::string, std::string> newProperties;
  newProperties["prefix"] = "UPDATED";
  auto newConfig = std::make_shared<FunctionCatalogConfig>(
      "test_catalog",
      std::make_shared<const velox::config::ConfigBase>(std::move(newProperties)));
  
  FunctionCatalogManager::instance()->registerCatalog("test_catalog", newConfig);

  // Test with updated config
  auto result2 = evaluateOnce<std::string>(
      "\"test_catalog.default.dynamic_func\"(c0)", 
      std::optional<std::string>("value"));
  EXPECT_EQ(result2, "UPDATED:value");
}

TEST_F(FunctionCatalogIntegrationTest, MultipleCatalogsWithSameFunctionName) {
  // Register another catalog
  std::unordered_map<std::string, std::string> props2;
  props2["prefix"] = "CATALOG2";
  auto config2 = std::make_shared<FunctionCatalogConfig>(
      "catalog2",
      std::make_shared<const velox::config::ConfigBase>(std::move(props2)));
  FunctionCatalogManager::instance()->registerCatalog("catalog2", config2);

  // Register same function name in different catalogs
  registerCatalogFunction<TestCatalogFunction, Varchar, Varchar>(
      "test_catalog", "default", "shared_func");
  registerCatalogFunction<TestCatalogFunction, Varchar, Varchar>(
      "catalog2", "default", "shared_func");

  // Test both functions
  auto result1 = evaluateOnce<std::string>(
      "\"test_catalog.default.shared_func\"(c0)", 
      std::optional<std::string>("data"));
  EXPECT_EQ(result1, "TEST:data");

  auto result2 = evaluateOnce<std::string>(
      "\"catalog2.default.shared_func\"(c0)", 
      std::optional<std::string>("data"));
  EXPECT_EQ(result2, "CATALOG2:data");
}

TEST_F(FunctionCatalogIntegrationTest, FunctionWithNullInput) {
  registerCatalogFunction<TestCatalogFunction, Varchar, Varchar>(
      "test_catalog", "default", "null_test");

  // Test with null input
  auto result = evaluateOnce<std::string>(
      "\"test_catalog.default.null_test\"(c0)", 
      std::optional<std::string>(std::nullopt));

  EXPECT_EQ(result, std::nullopt);
}

TEST_F(FunctionCatalogIntegrationTest, FunctionWithEmptyInput) {
  registerCatalogFunction<TestCatalogFunction, Varchar, Varchar>(
      "test_catalog", "default", "empty_test");

  // Test with empty string
  auto result = evaluateOnce<std::string>(
      "\"test_catalog.default.empty_test\"(c0)", 
      std::optional<std::string>(""));

  EXPECT_EQ(result, "TEST:");
}

TEST_F(FunctionCatalogIntegrationTest, ConfigPropertyTypes) {
  // Verify different property types are accessible
  auto* manager = FunctionCatalogManager::instance();
  auto config = manager->getCatalogConfig("test_catalog");
  
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(config->optionalProperty<std::string>("prefix"), "TEST");
  EXPECT_EQ(config->optionalProperty<int>("multiplier"), 2);
  EXPECT_EQ(config->optionalProperty<bool>("enabled"), true);
}

} // namespace facebook::presto::functions::test

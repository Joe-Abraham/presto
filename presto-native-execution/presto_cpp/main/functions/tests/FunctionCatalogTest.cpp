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

#include "presto_cpp/main/functions/FunctionCatalogConfig.h"
#include "presto_cpp/main/functions/FunctionCatalogManager.h"
#include <gtest/gtest.h>
#include "velox/common/config/Config.h"

using namespace facebook::presto;
using namespace facebook::velox;

class FunctionCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Clear any existing catalogs
    FunctionCatalogManager::instance()->clear();
  }

  void TearDown() override {
    FunctionCatalogManager::instance()->clear();
  }
};

TEST_F(FunctionCatalogTest, BasicCatalogRegistration) {
  std::unordered_map<std::string, std::string> properties;
  properties["test.property"] = "test_value";
  properties["test.number"] = "42";

  auto configBase =
      std::make_shared<const config::ConfigBase>(std::move(properties));
  auto config =
      std::make_shared<FunctionCatalogConfig>("test_catalog", configBase);

  auto* manager = FunctionCatalogManager::instance();
  manager->registerCatalog("test_catalog", config);

  EXPECT_TRUE(manager->hasCatalog("test_catalog"));
  EXPECT_FALSE(manager->hasCatalog("non_existent"));

  auto retrievedConfig = manager->getCatalogConfig("test_catalog");
  ASSERT_NE(retrievedConfig, nullptr);
  EXPECT_EQ(retrievedConfig->catalogName(), "test_catalog");
}

TEST_F(FunctionCatalogTest, CatalogConfigProperties) {
  std::unordered_map<std::string, std::string> properties;
  properties["string.prop"] = "value";
  properties["int.prop"] = "123";
  properties["bool.prop"] = "true";

  auto configBase =
      std::make_shared<const config::ConfigBase>(std::move(properties));
  auto config = std::make_shared<FunctionCatalogConfig>("test", configBase);

  // Test property access
  EXPECT_EQ(config->optionalProperty<std::string>("string.prop"), "value");
  EXPECT_EQ(config->optionalProperty<int>("int.prop"), 123);
  EXPECT_EQ(config->optionalProperty<bool>("bool.prop"), true);

  // Test missing property
  EXPECT_FALSE(config->optionalProperty<std::string>("missing.prop").has_value());

  // Test propertyOrDefault
  EXPECT_EQ(config->propertyOrDefault<std::string>("missing.prop", "default"), "default");
}

TEST_F(FunctionCatalogTest, SessionPropertyOverride) {
  std::unordered_map<std::string, std::string> baseProperties;
  baseProperties["prop1"] = "base_value";
  baseProperties["prop2"] = "original";

  auto configBase =
      std::make_shared<const config::ConfigBase>(std::move(baseProperties));
  auto config = std::make_shared<FunctionCatalogConfig>("test", configBase);

  // Apply session properties
  std::unordered_map<std::string, std::string> sessionProps;
  sessionProps["prop1"] = "session_value";  // Override
  sessionProps["prop3"] = "new_value";      // Add new

  auto configWithSession = config->withSessionProperties(sessionProps);

  // Original config should be unchanged
  EXPECT_EQ(config->optionalProperty<std::string>("prop1"), "base_value");
  EXPECT_EQ(config->optionalProperty<std::string>("prop2"), "original");

  // New config should have merged properties
  EXPECT_EQ(configWithSession->optionalProperty<std::string>("prop1"), "session_value");
  EXPECT_EQ(configWithSession->optionalProperty<std::string>("prop2"), "original");
  EXPECT_EQ(configWithSession->optionalProperty<std::string>("prop3"), "new_value");
}

TEST_F(FunctionCatalogTest, MultipleCatalogs) {
  auto* manager = FunctionCatalogManager::instance();

  // Register multiple catalogs
  for (int i = 0; i < 3; i++) {
    std::unordered_map<std::string, std::string> props;
    props["id"] = std::to_string(i);

    auto configBase = std::make_shared<const config::ConfigBase>(std::move(props));
    auto config = std::make_shared<FunctionCatalogConfig>(
        "catalog_" + std::to_string(i), configBase);
    manager->registerCatalog("catalog_" + std::to_string(i), config);
  }

  auto catalogNames = manager->getCatalogNames();
  EXPECT_EQ(catalogNames.size(), 3);

  // Verify all catalogs are accessible
  for (int i = 0; i < 3; i++) {
    auto name = "catalog_" + std::to_string(i);
    EXPECT_TRUE(manager->hasCatalog(name));
    auto config = manager->getCatalogConfig(name);
    ASSERT_NE(config, nullptr);
    EXPECT_EQ(config->optionalProperty<int>("id"), i);
  }
}

TEST_F(FunctionCatalogTest, GetCatalogConfigWithSession) {
  auto* manager = FunctionCatalogManager::instance();

  std::unordered_map<std::string, std::string> baseProps;
  baseProps["api.key"] = "base_key";
  baseProps["timeout"] = "30";

  auto configBase = std::make_shared<const config::ConfigBase>(std::move(baseProps));
  auto config = std::make_shared<FunctionCatalogConfig>("ai", configBase);
  manager->registerCatalog("ai", config);

  // Get config with session properties
  std::unordered_map<std::string, std::string> sessionProps;
  sessionProps["api.key"] = "session_key";
  sessionProps["max.tokens"] = "1000";

  auto sessionConfig = manager->getCatalogConfigWithSession("ai", sessionProps);
  ASSERT_NE(sessionConfig, nullptr);

  EXPECT_EQ(sessionConfig->optionalProperty<std::string>("api.key"), "session_key");
  EXPECT_EQ(sessionConfig->optionalProperty<int>("timeout"), 30);
  EXPECT_EQ(sessionConfig->optionalProperty<int>("max.tokens"), 1000);
}

TEST_F(FunctionCatalogTest, RequiredProperty) {
  std::unordered_map<std::string, std::string> properties;
  properties["required.prop"] = "value";

  auto configBase =
      std::make_shared<const config::ConfigBase>(std::move(properties));
  auto config = std::make_shared<FunctionCatalogConfig>("test", configBase);

  // Should succeed for existing property
  EXPECT_EQ(config->requiredProperty<std::string>("required.prop"), "value");

  // Should throw for missing property
  EXPECT_THROW(
      config->requiredProperty<std::string>("missing.prop"),
      velox::VeloxUserError);
}

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
#include "presto_cpp/main/functions/FunctionCatalogConfig.h"
#include "presto_cpp/main/functions/FunctionCatalogManager.h"
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
  EXPECT_FALSE(
      config->optionalProperty<std::string>("missing.prop").has_value());

  // Test propertyOrDefault
  EXPECT_EQ(
      config->propertyOrDefault<std::string>("missing.prop", "default"),
      "default");
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
  sessionProps["prop1"] = "session_value"; // Override
  sessionProps["prop3"] = "new_value"; // Add new

  auto configWithSession = config->withSessionProperties(sessionProps);

  // Original config should be unchanged
  EXPECT_EQ(config->optionalProperty<std::string>("prop1"), "base_value");
  EXPECT_EQ(config->optionalProperty<std::string>("prop2"), "original");

  // New config should have merged properties
  EXPECT_EQ(
      configWithSession->optionalProperty<std::string>("prop1"),
      "session_value");
  EXPECT_EQ(
      configWithSession->optionalProperty<std::string>("prop2"), "original");
  EXPECT_EQ(
      configWithSession->optionalProperty<std::string>("prop3"), "new_value");
}

TEST_F(FunctionCatalogTest, MultipleCatalogs) {
  auto* manager = FunctionCatalogManager::instance();

  // Register multiple catalogs
  for (int i = 0; i < 3; i++) {
    std::unordered_map<std::string, std::string> props;
    props["id"] = std::to_string(i);

    auto configBase =
        std::make_shared<const config::ConfigBase>(std::move(props));
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

  auto configBase =
      std::make_shared<const config::ConfigBase>(std::move(baseProps));
  auto config = std::make_shared<FunctionCatalogConfig>("ai", configBase);
  manager->registerCatalog("ai", config);

  // Get config with session properties
  std::unordered_map<std::string, std::string> sessionProps;
  sessionProps["api.key"] = "session_key";
  sessionProps["max.tokens"] = "1000";

  auto sessionConfig = manager->getCatalogConfigWithSession("ai", sessionProps);
  ASSERT_NE(sessionConfig, nullptr);

  EXPECT_EQ(
      sessionConfig->optionalProperty<std::string>("api.key"), "session_key");
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

TEST_F(FunctionCatalogTest, EmptyCatalogName) {
  std::unordered_map<std::string, std::string> properties;
  properties["test.prop"] = "value";

  auto configBase =
      std::make_shared<const config::ConfigBase>(std::move(properties));
  auto config = std::make_shared<FunctionCatalogConfig>("", configBase);

  auto* manager = FunctionCatalogManager::instance();
  manager->registerCatalog("", config);

  EXPECT_TRUE(manager->hasCatalog(""));
  EXPECT_EQ(config->catalogName(), "");
}

TEST_F(FunctionCatalogTest, CatalogOverwrite) {
  auto* manager = FunctionCatalogManager::instance();

  // Register first catalog
  std::unordered_map<std::string, std::string> props1;
  props1["version"] = "1";
  auto config1 = std::make_shared<FunctionCatalogConfig>(
      "test", std::make_shared<const config::ConfigBase>(std::move(props1)));
  manager->registerCatalog("test", config1);

  // Overwrite with second catalog
  std::unordered_map<std::string, std::string> props2;
  props2["version"] = "2";
  auto config2 = std::make_shared<FunctionCatalogConfig>(
      "test", std::make_shared<const config::ConfigBase>(std::move(props2)));
  manager->registerCatalog("test", config2);

  // Should have the second version
  auto retrieved = manager->getCatalogConfig("test");
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->optionalProperty<std::string>("version"), "2");
}

TEST_F(FunctionCatalogTest, PropertyTypeMismatch) {
  std::unordered_map<std::string, std::string> properties;
  properties["int.prop"] = "not_a_number";

  auto configBase =
      std::make_shared<const config::ConfigBase>(std::move(properties));
  auto config = std::make_shared<FunctionCatalogConfig>("test", configBase);

  // Should throw when trying to parse as int
  EXPECT_THROW(config->optionalProperty<int>("int.prop"), std::exception);
}

TEST_F(FunctionCatalogTest, SessionPropertiesEmptyMerge) {
  std::unordered_map<std::string, std::string> baseProps;
  baseProps["prop1"] = "value1";

  auto config = std::make_shared<FunctionCatalogConfig>(
      "test", std::make_shared<const config::ConfigBase>(std::move(baseProps)));

  // Merge with empty session properties
  std::unordered_map<std::string, std::string> emptySession;
  auto merged = config->withSessionProperties(emptySession);

  // Should still have original properties
  EXPECT_EQ(merged->optionalProperty<std::string>("prop1"), "value1");
}

TEST_F(FunctionCatalogTest, CatalogConfigImmutability) {
  std::unordered_map<std::string, std::string> baseProps;
  baseProps["prop1"] = "original";

  auto config = std::make_shared<FunctionCatalogConfig>(
      "test", std::make_shared<const config::ConfigBase>(std::move(baseProps)));

  // Create session-merged config
  std::unordered_map<std::string, std::string> sessionProps;
  sessionProps["prop1"] = "modified";
  auto merged = config->withSessionProperties(sessionProps);

  // Original should be unchanged
  EXPECT_EQ(config->optionalProperty<std::string>("prop1"), "original");
  // Merged should have new value
  EXPECT_EQ(merged->optionalProperty<std::string>("prop1"), "modified");
}

TEST_F(FunctionCatalogTest, NonExistentCatalogReturnsNull) {
  auto* manager = FunctionCatalogManager::instance();
  
  auto config = manager->getCatalogConfig("nonexistent");
  EXPECT_EQ(config, nullptr);

  auto configWithSession = manager->getCatalogConfigWithSession(
      "nonexistent", {{"key", "value"}});
  EXPECT_EQ(configWithSession, nullptr);
}

TEST_F(FunctionCatalogTest, CatalogNamesCasePreserving) {
  auto* manager = FunctionCatalogManager::instance();

  std::unordered_map<std::string, std::string> props;
  props["test"] = "value";
  auto config = std::make_shared<FunctionCatalogConfig>(
      "MixedCase",
      std::make_shared<const config::ConfigBase>(std::move(props)));

  manager->registerCatalog("MixedCase", config);

  // Case-sensitive lookup
  EXPECT_TRUE(manager->hasCatalog("MixedCase"));
  EXPECT_FALSE(manager->hasCatalog("mixedcase"));
  EXPECT_FALSE(manager->hasCatalog("MIXEDCASE"));
}

TEST_F(FunctionCatalogTest, LargeConfigurationValues) {
  std::unordered_map<std::string, std::string> properties;
  
  // Test with large string value
  std::string largeValue(10000, 'x');
  properties["large.value"] = largeValue;
  properties["int.value"] = "999999999";

  auto config = std::make_shared<FunctionCatalogConfig>(
      "test", std::make_shared<const config::ConfigBase>(std::move(properties)));

  EXPECT_EQ(config->optionalProperty<std::string>("large.value"), largeValue);
  EXPECT_EQ(config->optionalProperty<int>("int.value"), 999999999);
}

TEST_F(FunctionCatalogTest, SpecialCharactersInKeys) {
  std::unordered_map<std::string, std::string> properties;
  properties["key-with-dashes"] = "value1";
  properties["key.with.dots"] = "value2";
  properties["key_with_underscores"] = "value3";

  auto config = std::make_shared<FunctionCatalogConfig>(
      "test", std::make_shared<const config::ConfigBase>(std::move(properties)));

  EXPECT_EQ(
      config->optionalProperty<std::string>("key-with-dashes"), "value1");
  EXPECT_EQ(config->optionalProperty<std::string>("key.with.dots"), "value2");
  EXPECT_EQ(
      config->optionalProperty<std::string>("key_with_underscores"), "value3");
}

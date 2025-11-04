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
#include <filesystem>
#include <fstream>
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/functions/FunctionCatalogConfig.h"
#include "presto_cpp/main/functions/FunctionCatalogManager.h"
#include "velox/common/config/Config.h"

using namespace facebook::presto;
using namespace facebook::velox;
namespace fs = std::filesystem;

namespace facebook::presto::functions::test {

class FunctionCatalogE2ETest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create temporary test directory
    testDir_ = fs::temp_directory_path() / "presto_catalog_test";
    fs::create_directories(testDir_);

    // Clear any existing catalogs
    FunctionCatalogManager::instance()->clear();
  }

  void TearDown() override {
    // Clean up test directory
    if (fs::exists(testDir_)) {
      fs::remove_all(testDir_);
    }
    FunctionCatalogManager::instance()->clear();
  }

  void createCatalogFile(
      const std::string& catalogName,
      const std::unordered_map<std::string, std::string>& properties) {
    fs::path filePath = testDir_ / (catalogName + ".properties");
    std::ofstream file(filePath);
    for (const auto& [key, value] : properties) {
      file << key << "=" << value << "\n";
    }
    file.close();
  }

  void loadCatalogsFromDirectory() {
    static const std::string kPropertiesExtension = ".properties";
    auto* manager = FunctionCatalogManager::instance();

    for (const auto& entry : fs::directory_iterator(testDir_)) {
      if (entry.path().extension() == kPropertiesExtension) {
        auto fileName = entry.path().filename().string();
        auto catalogName = fileName.substr(
            0, fileName.size() - kPropertiesExtension.size());

        auto catalogConf = util::readConfig(entry.path());
        std::shared_ptr<const config::ConfigBase> properties =
            std::make_shared<const config::ConfigBase>(std::move(catalogConf));

        auto catalogConfig =
            std::make_shared<FunctionCatalogConfig>(catalogName, properties);
        manager->registerCatalog(catalogName, catalogConfig);
      }
    }
  }

  fs::path testDir_;
};

TEST_F(FunctionCatalogE2ETest, LoadSingleCatalogFromFile) {
  // Create a catalog file
  createCatalogFile("test_catalog", {
      {"test.property", "test_value"},
      {"numeric.property", "42"},
      {"bool.property", "true"}
  });

  // Load catalogs
  loadCatalogsFromDirectory();

  // Verify catalog was loaded
  auto* manager = FunctionCatalogManager::instance();
  EXPECT_TRUE(manager->hasCatalog("test_catalog"));

  auto config = manager->getCatalogConfig("test_catalog");
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(config->optionalProperty<std::string>("test.property"), "test_value");
  EXPECT_EQ(config->optionalProperty<int>("numeric.property"), 42);
  EXPECT_EQ(config->optionalProperty<bool>("bool.property"), true);
}

TEST_F(FunctionCatalogE2ETest, LoadMultipleCatalogs) {
  // Create multiple catalog files
  createCatalogFile("catalog1", {{"id", "1"}});
  createCatalogFile("catalog2", {{"id", "2"}});
  createCatalogFile("catalog3", {{"id", "3"}});

  // Load catalogs
  loadCatalogsFromDirectory();

  // Verify all catalogs were loaded
  auto* manager = FunctionCatalogManager::instance();
  auto catalogNames = manager->getCatalogNames();
  EXPECT_EQ(catalogNames.size(), 3);

  EXPECT_TRUE(manager->hasCatalog("catalog1"));
  EXPECT_TRUE(manager->hasCatalog("catalog2"));
  EXPECT_TRUE(manager->hasCatalog("catalog3"));
}

TEST_F(FunctionCatalogE2ETest, AIFunctionCatalogExample) {
  // Create an AI catalog similar to the use case in the PR
  createCatalogFile("ai", {
      {"ai.openai.api-key", "test-api-key"},
      {"ai.openai.model", "gpt-4"},
      {"ai.openai.max-tokens", "2000"},
      {"ai.openai.temperature", "0.7"}
  });

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  auto config = manager->getCatalogConfig("ai");
  
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(
      config->optionalProperty<std::string>("ai.openai.api-key"),
      "test-api-key");
  EXPECT_EQ(config->optionalProperty<std::string>("ai.openai.model"), "gpt-4");
  EXPECT_EQ(config->optionalProperty<int>("ai.openai.max-tokens"), 2000);
  
  // Test session override
  std::unordered_map<std::string, std::string> sessionProps;
  sessionProps["ai.openai.api-key"] = "user-api-key";
  sessionProps["ai.openai.temperature"] = "0.9";

  auto sessionConfig = config->withSessionProperties(sessionProps);
  EXPECT_EQ(
      sessionConfig->optionalProperty<std::string>("ai.openai.api-key"),
      "user-api-key");
  EXPECT_EQ(
      sessionConfig->optionalProperty<std::string>("ai.openai.model"),
      "gpt-4"); // unchanged
  EXPECT_EQ(
      sessionConfig->optionalProperty<std::string>("ai.openai.temperature"),
      "0.9"); // overridden
}

TEST_F(FunctionCatalogE2ETest, EmptyPropertiesFile) {
  // Create an empty properties file
  createCatalogFile("empty_catalog", {});

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  EXPECT_TRUE(manager->hasCatalog("empty_catalog"));
  
  auto config = manager->getCatalogConfig("empty_catalog");
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(config->catalogName(), "empty_catalog");
}

TEST_F(FunctionCatalogE2ETest, PropertiesWithComments) {
  // Manually create a file with comments
  fs::path filePath = testDir_ / "commented.properties";
  std::ofstream file(filePath);
  file << "# This is a comment\n";
  file << "property1=value1\n";
  file << "# Another comment\n";
  file << "property2=value2\n";
  file.close();

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  auto config = manager->getCatalogConfig("commented");
  
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(config->optionalProperty<std::string>("property1"), "value1");
  EXPECT_EQ(config->optionalProperty<std::string>("property2"), "value2");
}

TEST_F(FunctionCatalogE2ETest, PropertiesWithSpecialCharacters) {
  createCatalogFile("special", {
      {"url", "https://example.com:8080/path"},
      {"path.with.dots", "/home/user/data"},
      {"key-with-dashes", "value-with-dashes"},
      {"spaces_value", "value with spaces"}
  });

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  auto config = manager->getCatalogConfig("special");
  
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(
      config->optionalProperty<std::string>("url"),
      "https://example.com:8080/path");
  EXPECT_EQ(
      config->optionalProperty<std::string>("path.with.dots"),
      "/home/user/data");
  EXPECT_EQ(
      config->optionalProperty<std::string>("key-with-dashes"),
      "value-with-dashes");
  EXPECT_EQ(
      config->optionalProperty<std::string>("spaces_value"),
      "value with spaces");
}

TEST_F(FunctionCatalogE2ETest, CatalogNameFromFilename) {
  // Test that catalog name is derived from filename
  createCatalogFile("my-custom-catalog", {{"test", "value"}});
  createCatalogFile("CamelCaseCatalog", {{"test", "value"}});
  createCatalogFile("catalog_with_underscores", {{"test", "value"}});

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  EXPECT_TRUE(manager->hasCatalog("my-custom-catalog"));
  EXPECT_TRUE(manager->hasCatalog("CamelCaseCatalog"));
  EXPECT_TRUE(manager->hasCatalog("catalog_with_underscores"));
}

TEST_F(FunctionCatalogE2ETest, LargePropertiesFile) {
  // Create a catalog with many properties
  std::unordered_map<std::string, std::string> props;
  for (int i = 0; i < 100; i++) {
    props["property." + std::to_string(i)] = "value_" + std::to_string(i);
  }
  createCatalogFile("large", props);

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  auto config = manager->getCatalogConfig("large");
  
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(config->optionalProperty<std::string>("property.0"), "value_0");
  EXPECT_EQ(config->optionalProperty<std::string>("property.50"), "value_50");
  EXPECT_EQ(config->optionalProperty<std::string>("property.99"), "value_99");
}

TEST_F(FunctionCatalogE2ETest, NonPropertiesFilesIgnored) {
  // Create various non-.properties files
  fs::path txtFile = testDir_ / "readme.txt";
  std::ofstream(txtFile) << "This should be ignored\n";
  
  fs::path mdFile = testDir_ / "doc.md";
  std::ofstream(mdFile) << "# Documentation\n";

  // Create one valid properties file
  createCatalogFile("valid", {{"test", "value"}});

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  auto catalogNames = manager->getCatalogNames();
  
  // Should only have the valid catalog
  EXPECT_EQ(catalogNames.size(), 1);
  EXPECT_TRUE(manager->hasCatalog("valid"));
}

TEST_F(FunctionCatalogE2ETest, SessionPropertiesE2E) {
  // Create a catalog
  createCatalogFile("session_test", {
      {"default.timeout", "30"},
      {"default.retries", "3"},
      {"api.endpoint", "https://api.example.com"}
  });

  loadCatalogsFromDirectory();

  auto* manager = FunctionCatalogManager::instance();
  
  // Simulate user session overrides
  std::unordered_map<std::string, std::string> userSession;
  userSession["default.timeout"] = "60";  // User wants longer timeout
  userSession["user.id"] = "12345";       // User-specific property

  auto sessionConfig = manager->getCatalogConfigWithSession(
      "session_test", userSession);
  
  ASSERT_NE(sessionConfig, nullptr);
  EXPECT_EQ(sessionConfig->optionalProperty<int>("default.timeout"), 60);
  EXPECT_EQ(sessionConfig->optionalProperty<int>("default.retries"), 3);
  EXPECT_EQ(
      sessionConfig->optionalProperty<std::string>("api.endpoint"),
      "https://api.example.com");
  EXPECT_EQ(sessionConfig->optionalProperty<std::string>("user.id"), "12345");
}

} // namespace facebook::presto::functions::test

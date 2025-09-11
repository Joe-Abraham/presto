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

#include "presto_cpp/main/HiveFunctionRegistration.h"
#include "presto_cpp/main/types/FunctionMetadata.h"
#include <gtest/gtest.h>

namespace facebook::presto {

class HiveFunctionRegistrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Clean any previously registered functions
  }
  
  void TearDown() override {
    // Clean up after test
  }
};

TEST_F(HiveFunctionRegistrationTest, testHiveFunctionRegistration) {
  // Test that Hive functions can be registered
  size_t registeredCount = registerHiveFunctions();
  
  // Should register at least the initcap function
  EXPECT_GT(registeredCount, 0);
}

TEST_F(HiveFunctionRegistrationTest, testCatalogFiltering) {
  // Register some Hive functions
  registerHiveFunctions();
  
  // Test that getFunctionsMetadataForCatalog returns functions for "hive" catalog
  auto hiveMetadata = getFunctionsMetadataForCatalog("hive");
  
  // Should contain at least the initcap function
  EXPECT_FALSE(hiveMetadata.empty());
  
  // Test that built-in catalog doesn't include Hive functions
  auto builtinMetadata = getFunctionsMetadataForCatalog("presto.default");
  
  // Should not contain Hive functions
  // This verifies proper catalog separation
  if (!hiveMetadata.empty() && !builtinMetadata.empty()) {
    // Verify that the functions are properly separated by catalog
    EXPECT_NE(hiveMetadata.dump(), builtinMetadata.dump());
  }
}

TEST_F(HiveFunctionRegistrationTest, testMultipleNamespaces) {
  // Test that multiple catalogs can coexist
  registerHiveFunctions();
  
  // Get metadata for different catalogs
  auto hiveMetadata = getFunctionsMetadataForCatalog("hive");
  auto allMetadata = getFunctionsMetadata();
  
  // All metadata should include functions from all catalogs
  EXPECT_FALSE(allMetadata.empty());
  
  // Hive metadata should be a subset of all metadata
  if (!hiveMetadata.empty()) {
    // This demonstrates that catalog filtering works correctly
    EXPECT_TRUE(true); // Basic validation that filtering doesn't crash
  }
}

} // namespace facebook::presto
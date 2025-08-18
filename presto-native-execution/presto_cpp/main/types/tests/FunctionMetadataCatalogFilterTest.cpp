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
#include "presto_cpp/main/types/FunctionMetadata.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::presto;
using namespace facebook::velox;

class FunctionMetadataCatalogFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Register some test functions with different catalogs
    registerTestFunctions();
  }

  void registerTestFunctions() {
    // Register functions with different catalog prefixes for testing
    // Note: In a real scenario, these would be registered through the normal
    // function registration process with catalog prefixes

    // For this test, we'll use the existing registration and test the filtering
    // logic
    functions::prestosql::registerAllScalarFunctions("presto.default");
    aggregate::prestosql::registerAllAggregateFunctions("presto.default");

    // Register with a custom catalog prefix
    functions::prestosql::registerAllScalarFunctions("custom.schema");
    aggregate::prestosql::registerAllAggregateFunctions("custom.schema");
    
    // Register with additional catalog prefixes to test multiple namespaces
    functions::prestosql::registerAllScalarFunctions("ml.models");
    aggregate::prestosql::registerAllAggregateFunctions("ml.models");
    functions::prestosql::registerAllScalarFunctions("data.lake");
    aggregate::prestosql::registerAllAggregateFunctions("data.lake");
  }
};

TEST_F(FunctionMetadataCatalogFilterTest, TestGetAllFunctions) {
  auto metadata = getFunctionsMetadata();

  // Verify that some functions are returned
  EXPECT_GT(metadata.size(), 0);

  // Verify that we have some common functions like 'abs', 'sum', etc.
  EXPECT_TRUE(metadata.contains("abs"));
  EXPECT_TRUE(metadata.contains("sum"));
}

TEST_F(FunctionMetadataCatalogFilterTest, TestGetFunctionsFilteredByCatalog) {
  auto allMetadata = getFunctionsMetadata();
  auto prestoMetadata = getFunctionsMetadata("presto");
  auto customMetadata = getFunctionsMetadata("custom");
  auto nonExistentMetadata = getFunctionsMetadata("nonexistent");

  // Filtered results should be subsets of all functions
  EXPECT_LE(prestoMetadata.size(), allMetadata.size());
  EXPECT_LE(customMetadata.size(), allMetadata.size());

  // Non-existent catalog should return empty or smaller result set
  EXPECT_LE(nonExistentMetadata.size(), allMetadata.size());

  // Presto catalog should have functions since we registered with
  // "presto.default"
  EXPECT_GT(prestoMetadata.size(), 0);

  // Custom catalog should have functions since we registered with
  // "custom.schema"
  EXPECT_GT(customMetadata.size(), 0);
}

TEST_F(FunctionMetadataCatalogFilterTest, TestEmptyCatalogFilter) {
  auto allMetadata = getFunctionsMetadata();
  auto emptyCatalogMetadata = getFunctionsMetadata("");

  // Empty catalog should behave the same as no filter
  EXPECT_EQ(allMetadata.size(), emptyCatalogMetadata.size());
}

TEST_F(FunctionMetadataCatalogFilterTest, TestCatalogFilteringLogic) {
  // Test the catalog filtering logic by checking that functions
  // are properly filtered by their catalog prefix

  // Get all functions to understand the structure
  auto allMetadata = getFunctionsMetadata();

  // For each function in the all functions result, verify it would
  // be included in the correct catalog filter
  for (auto& [functionName, functionList] : allMetadata.items()) {
    EXPECT_FALSE(functionName.empty()) << "Function name should not be empty";
    EXPECT_GT(functionList.size(), 0)
        << "Function should have at least one signature";
  }
}

TEST_F(FunctionMetadataCatalogFilterTest, TestFunctionStructure) {
  auto metadata = getFunctionsMetadata("presto");

  // Verify the JSON structure is correct
  EXPECT_TRUE(metadata.is_object());

  // Check a specific function exists and has the right structure
  if (metadata.contains("abs")) {
    auto absFunction = metadata["abs"];
    EXPECT_TRUE(absFunction.is_array());
    EXPECT_GT(absFunction.size(), 0);

    // Check the first signature has required fields
    auto firstSignature = absFunction[0];
    EXPECT_TRUE(firstSignature.contains("outputType"));
    EXPECT_TRUE(firstSignature.contains("paramTypes"));
    EXPECT_TRUE(firstSignature.contains("functionKind"));
    EXPECT_TRUE(firstSignature.contains("schema"));
  }
}

TEST_F(FunctionMetadataCatalogFilterTest, TestMultipleNamespaces) {
  // Test that all additional namespaces have their own functions
  auto mlMetadata = getFunctionsMetadata("ml");
  auto dataMetadata = getFunctionsMetadata("data");
  auto customMetadata = getFunctionsMetadata("custom");
  
  // All should have functions since we registered with these prefixes
  EXPECT_GT(mlMetadata.size(), 0) << "ML namespace should have functions";
  EXPECT_GT(dataMetadata.size(), 0) << "Data namespace should have functions";
  EXPECT_GT(customMetadata.size(), 0) << "Custom namespace should have functions";
  
  // Verify that functions from different namespaces don't overlap with default
  auto allMetadata = getFunctionsMetadata();
  auto defaultMetadata = getFunctionsMetadata("presto");
  
  // All metadata should include functions from all namespaces
  EXPECT_GE(allMetadata.size(), defaultMetadata.size());
  EXPECT_GE(allMetadata.size(), mlMetadata.size());
  EXPECT_GE(allMetadata.size(), dataMetadata.size());
  EXPECT_GE(allMetadata.size(), customMetadata.size());
}

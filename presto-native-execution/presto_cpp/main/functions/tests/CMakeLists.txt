# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(presto_function_metadata_test FunctionMetadataTest.cpp)

add_test(
  NAME presto_function_metadata_test
  COMMAND presto_function_metadata_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  presto_function_metadata_test
  presto_function_metadata
  presto_protocol
  presto_type_test_utils
  velox_aggregates
  velox_functions_prestosql
  velox_window
  GTest::gtest
  GTest::gtest_main
)

add_executable(presto_function_catalog_test FunctionCatalogTest.cpp)

add_test(
  NAME presto_function_catalog_test
  COMMAND presto_function_catalog_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  presto_function_catalog_test
  presto_function_metadata
  velox_config
  GTest::gtest
  GTest::gtest_main
)

add_executable(presto_function_catalog_integration_test
               FunctionCatalogIntegrationTest.cpp)

add_test(
  NAME presto_function_catalog_integration_test
  COMMAND presto_function_catalog_integration_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  presto_function_catalog_integration_test
  presto_function_metadata
  velox_config
  velox_functions_prestosql
  velox_functions_test_lib
  GTest::gtest
  GTest::gtest_main
)

add_executable(presto_function_catalog_e2e_test FunctionCatalogE2ETest.cpp)

add_test(
  NAME presto_function_catalog_e2e_test
  COMMAND presto_function_catalog_e2e_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  presto_function_catalog_e2e_test
  presto_function_metadata
  presto_common
  velox_config
  GTest::gtest
  GTest::gtest_main
)



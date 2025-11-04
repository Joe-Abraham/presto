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

// This is an example showing how to create a dynamic function that uses
// catalog configuration. This demonstrates the function catalog feature
// which allows organizing functions with shared configuration.

#include <string>
#include "presto_cpp/main/functions/FunctionCatalogManager.h"
#include "presto_cpp/main/functions/dynamic_registry/CatalogFunctionRegistrar.h"
#include "velox/functions/Macros.h"

using namespace facebook::velox;
using namespace facebook::presto;

// Example function that uses catalog configuration
template <typename T>
struct CatalogConfigAwareFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    // Access catalog configuration at runtime
    auto* catalogManager = FunctionCatalogManager::instance();
    auto config = catalogManager->getCatalogConfig("examples");

    std::string prefix = "default";
    if (config) {
      // Get configuration property with default value
      prefix = config->propertyOrDefault<std::string>(
          "example.prefix", "default");
    }

    // Use configuration in function logic
    result = prefix + ": " + input;
  }
};

// Function registration - this would typically be called from a plugin
// initialization function or at server startup
extern "C" void registerExampleCatalogFunction() {
  // Register function in the 'examples' catalog
  registerCatalogFunction<CatalogConfigAwareFunction, Varchar, Varchar>(
      "examples",           // catalog name
      "default",            // schema name
      "with_prefix"         // function name
  );
  // This registers the function as: examples.default.with_prefix
}

// Example usage:
// 1. Create etc/function-catalog/examples.properties:
//    example.prefix=MyPrefix
//
// 2. Query: SELECT "examples.default.with_prefix"('hello')
//    Result: "MyPrefix: hello"
//
// 3. Override via session:
//    SET SESSION examples.example_prefix = 'CustomPrefix'
//    SELECT "examples.default.with_prefix"('hello')
//    Result: "CustomPrefix: hello"

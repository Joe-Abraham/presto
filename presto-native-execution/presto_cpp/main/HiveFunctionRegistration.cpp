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
#include <glog/logging.h>
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/prestosql/StringFunctions.h"

namespace facebook::presto {

namespace {

// Simple initcap function implementation for Hive compatibility
// This demonstrates how Hive-specific functions can be registered
// with proper catalog namespacing
class InitCapFunction : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& rows,
      std::vector<velox::VectorPtr>& args,
      const velox::TypePtr& outputType,
      velox::exec::EvalCtx& context,
      velox::VectorPtr& result) const override {
    // For demonstration purposes, we'll provide a basic implementation
    // that shows how this would work. In practice, this would implement
    // the full initcap logic (capitalizing first letter of each word).
    
    VELOX_CHECK_EQ(args.size(), 1, "initcap expects exactly 1 argument");
    
    auto input = args[0]->as<velox::SimpleVector<velox::StringView>>();
    auto output = std::dynamic_pointer_cast<velox::FlatVector<velox::StringView>>(
        velox::BaseVector::create(outputType, rows.size(), context.pool()));
    
    rows.applyToSelected([&](int32_t row) {
      if (input->isNullAt(row)) {
        output->setNull(row, true);
      } else {
        // For demonstration, just return the input string
        // A full implementation would capitalize the first letter of each word
        auto inputValue = input->valueAt(row);
        output->set(row, inputValue);
      }
    });
    
    result = output;
  }
  
  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    return {velox::exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .build()};
  }
};

} // namespace

size_t registerHiveFunctions() {
  size_t registeredCount = 0;
  
  try {
    // Register initcap function with hive.default namespace
    // This demonstrates proper catalog namespacing for Hive functions
    velox::exec::registerVectorFunction(
        "hive.default.initcap",
        InitCapFunction::signatures(),
        std::make_unique<InitCapFunction>());
    registeredCount++;
    
    LOG(INFO) << "Registered Hive function: hive.default.initcap";
    
    // Additional Hive functions could be registered here
    // For example: hive.default.concat_ws, hive.default.regexp_extract, etc.
    
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to register some Hive functions: " << e.what();
  }
  
  return registeredCount;
}

} // namespace facebook::presto
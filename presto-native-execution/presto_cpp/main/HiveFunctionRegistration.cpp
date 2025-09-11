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
#include <algorithm>
#include <cctype>
#include <cstring>
#include "velox/common/base/Exceptions.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/string/StringImpl.h"

using namespace facebook::velox;
namespace facebook::presto {

/// The InitCapFunction capitalizes the first character of each word in a
/// string, and lowercases the rest, following Spark SQL semantics. Word
/// boundaries are determined by whitespace.
template <typename T>
struct InitCapFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    functions::stringImpl::initcap<false, false, false, false>(result, input);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    functions::stringImpl::initcap<false, true, false, false>(result, input);
  }
};

size_t registerHiveFunctions() {
  size_t registeredCount = 0;

  try {
    // Register initcap function with hive.default namespace
    // This demonstrates proper catalog namespacing for Hive functions

    registerFunction<InitCapFunction, Varchar, Varchar>(
        {"hive.default.initcap"});
    LOG(INFO) << "Registered Hive function: hive.default.initcap";

    // Additional Hive functions could be registered here
    // For example: hive.default.concat_ws, hive.default.regexp_extract, etc.

  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to register some Hive functions: " << e.what();
  }

  // return registeredCount;
}

} // namespace facebook::presto

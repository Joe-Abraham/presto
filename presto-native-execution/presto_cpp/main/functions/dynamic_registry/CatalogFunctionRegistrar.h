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
#pragma once

#include "presto_cpp/main/functions/FunctionCatalogConfig.h"
#include "presto_cpp/main/functions/FunctionCatalogManager.h"
#include "presto_cpp/main/functions/dynamic_registry/DynamicFunctionRegistrar.h"

namespace facebook::presto {

/// Register a function within a specific catalog namespace.
/// The function will be registered as "catalog.schema.functionName".
/// The catalog config can be accessed from FunctionCatalogManager during
/// function execution.
template <template <class> class T, typename TReturn, typename... TArgs>
void registerCatalogFunction(
    const std::string_view catalogName,
    const std::string_view schema,
    const std::string_view functionName,
    const std::vector<velox::exec::SignatureVariable>& constraints = {}) {
  std::string fullName;
  fullName.append(catalogName);
  fullName.append(".");
  fullName.append(schema);
  fullName.append(".");
  fullName.append(functionName);

  LOG(INFO) << "Registering catalog function: " << fullName;
  facebook::velox::registerFunction<T, TReturn, TArgs...>(
      {fullName}, constraints, false);
}

/// Register a function within a catalog using a default schema.
/// The function will be registered as "catalog.default.functionName".
template <template <class> class T, typename TReturn, typename... TArgs>
void registerCatalogFunction(
    const std::string_view catalogName,
    const std::string_view functionName,
    const std::vector<velox::exec::SignatureVariable>& constraints = {}) {
  registerCatalogFunction<T, TReturn, TArgs...>(
      catalogName, "default", functionName, constraints);
}

} // namespace facebook::presto

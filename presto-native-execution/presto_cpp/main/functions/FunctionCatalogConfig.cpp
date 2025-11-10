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

namespace facebook::presto {

std::shared_ptr<FunctionCatalogConfig>
FunctionCatalogConfig::withSessionProperties(
    const std::unordered_map<std::string, std::string>& sessionProperties)
    const {
  // Merge base properties with session properties
  auto baseConfig = properties_->rawConfigsCopy();
  for (const auto& [key, value] : sessionProperties) {
    baseConfig[key] = value;
  }

  auto mergedProperties =
      std::make_shared<const velox::config::ConfigBase>(std::move(baseConfig));

  return std::make_shared<FunctionCatalogConfig>(
      catalogName_, std::move(mergedProperties));
}

} // namespace facebook::presto

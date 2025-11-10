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
#include "presto_cpp/main/functions/FunctionCatalogManager.h"
#include <glog/logging.h>

namespace facebook::presto {

FunctionCatalogManager* FunctionCatalogManager::instance() {
  static std::unique_ptr<FunctionCatalogManager> instance =
      std::make_unique<FunctionCatalogManager>();
  return instance.get();
}

void FunctionCatalogManager::registerCatalog(
    const std::string& catalogName,
    std::shared_ptr<FunctionCatalogConfig> config) {
  std::lock_guard<std::mutex> lock(mutex_);
  LOG(INFO) << "Registering function catalog: " << catalogName;
  catalogs_[catalogName] = std::move(config);
}

std::shared_ptr<FunctionCatalogConfig> FunctionCatalogManager::getCatalogConfig(
    const std::string& catalogName) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = catalogs_.find(catalogName);
  if (it != catalogs_.end()) {
    return it->second;
  }
  return nullptr;
}

std::shared_ptr<FunctionCatalogConfig>
FunctionCatalogManager::getCatalogConfigWithSession(
    const std::string& catalogName,
    const std::unordered_map<std::string, std::string>& sessionProperties)
    const {
  auto baseConfig = getCatalogConfig(catalogName);
  if (!baseConfig) {
    return nullptr;
  }
  return baseConfig->withSessionProperties(sessionProperties);
}

std::vector<std::string> FunctionCatalogManager::getCatalogNames() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::string> names;
  names.reserve(catalogs_.size());
  for (const auto& [name, _] : catalogs_) {
    names.push_back(name);
  }
  return names;
}

bool FunctionCatalogManager::hasCatalog(const std::string& catalogName) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return catalogs_.find(catalogName) != catalogs_.end();
}

void FunctionCatalogManager::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  catalogs_.clear();
}

} // namespace facebook::presto

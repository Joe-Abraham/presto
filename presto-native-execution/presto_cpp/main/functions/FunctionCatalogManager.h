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

#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include "presto_cpp/main/functions/FunctionCatalogConfig.h"

namespace facebook::presto {

/// Manages function catalogs. Function catalogs group related functions
/// with shared configuration. This manager handles:
/// - Loading catalog configurations from .properties files
/// - Managing catalog lifecycle
/// - Providing catalog configs to function registration
/// - Supporting session-level config overrides
class FunctionCatalogManager {
 public:
  /// Returns the singleton instance.
  static FunctionCatalogManager* instance();

  /// Registers a function catalog with the given name and configuration.
  void registerCatalog(
      const std::string& catalogName,
      std::shared_ptr<FunctionCatalogConfig> config);

  /// Gets the configuration for a catalog. Returns nullptr if not found.
  std::shared_ptr<FunctionCatalogConfig> getCatalogConfig(
      const std::string& catalogName) const;

  /// Gets catalog config with session properties applied.
  std::shared_ptr<FunctionCatalogConfig> getCatalogConfigWithSession(
      const std::string& catalogName,
      const std::unordered_map<std::string, std::string>& sessionProperties)
      const;

  /// Returns all registered catalog names.
  std::vector<std::string> getCatalogNames() const;

  /// Checks if a catalog is registered.
  bool hasCatalog(const std::string& catalogName) const;

  /// Clears all registered catalogs (mainly for testing).
  void clear();

 private:
  FunctionCatalogManager() = default;

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<FunctionCatalogConfig>>
      catalogs_;
};

} // namespace facebook::presto

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

#include <memory>
#include <string>
#include <unordered_map>
#include "velox/common/config/Config.h"

namespace facebook::presto {

/// Configuration for a function catalog. Function catalogs organize related
/// functions with shared configuration, similar to connector catalogs.
/// Configuration can be loaded from .properties files and overridden by
/// session properties at runtime.
class FunctionCatalogConfig {
 public:
  FunctionCatalogConfig(
      const std::string& catalogName,
      std::shared_ptr<const velox::config::ConfigBase> properties)
      : catalogName_(catalogName), properties_(std::move(properties)) {}

  /// Returns the catalog name.
  const std::string& catalogName() const {
    return catalogName_;
  }

  /// Returns all configuration properties.
  const std::shared_ptr<const velox::config::ConfigBase>& properties() const {
    return properties_;
  }

  /// Gets a required property value. Throws if the property is not found.
  template <typename T>
  T requiredProperty(const std::string& key) const {
    auto value = properties_->get<T>(key);
    if (!value.has_value()) {
      VELOX_USER_FAIL(
          "Required property '{}' not found in function catalog '{}'",
          key,
          catalogName_);
    }
    return value.value();
  }

  /// Gets an optional property value. Returns folly::none if not found.
  template <typename T>
  folly::Optional<T> optionalProperty(const std::string& key) const {
    auto value = properties_->get<T>(key);
    if (value.has_value()) {
      return value.value();
    }
    return folly::none;
  }

  /// Gets a property value with a default. Returns the default if not found.
  template <typename T>
  T propertyOrDefault(const std::string& key, const T& defaultValue) const {
    auto value = properties_->get<T>(key);
    return value.value_or(defaultValue);
  }

  /// Creates a new config with session properties merged in.
  std::shared_ptr<FunctionCatalogConfig> withSessionProperties(
      const std::unordered_map<std::string, std::string>& sessionProperties)
      const;

 private:
  std::string catalogName_;
  std::shared_ptr<const velox::config::ConfigBase> properties_;
};

} // namespace facebook::presto

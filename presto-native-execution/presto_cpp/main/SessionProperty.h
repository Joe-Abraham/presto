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

#include <string>
#include <typeinfo>

namespace facebook::presto {

// Defines prossible property types.
enum class PropertyType {
  kInt,
  kBool,
  kLong,
  // Add more types as needed.
  kUnknown
};

/// This is the interface of the session property.
/// Note: This interface should align with java coordinator.
class SessionProperty {
 public:
  SessionProperty() = default;

  virtual ~SessionProperty() = default;

  virtual std::string getName() const = 0;

  virtual std::string getDescription() const = 0;

  virtual PropertyType getType() const = 0;

  virtual std::string getDefaultValue() const = 0;

  virtual bool isHidden() const = 0;
};

// Template class for different types of the session property.
template <class T>
class SessionPropertyData : public SessionProperty {
 public:
  // Restricting session properties without data.
  SessionPropertyData() = delete;

  SessionPropertyData(
      const std::string& name,
      const std::string& description,
      const T& default_value,
      const bool hidden)
      : name_(name),
        description_(description),
        default_value_(default_value),
        hidden_(hidden) {}

  inline std::string getName() const {
    return name_;
  }

  inline std::string getDescription() const {
    return description_;
  }

  inline PropertyType getType() const {
    const std::type_info& typeinfo = typeid(T);
    return (typeinfo == typeid(int)) ? PropertyType::kInt
        : (typeinfo == typeid(bool)) ? PropertyType::kBool
        : (typeinfo == typeid(long)) ? PropertyType::kLong
                                     : PropertyType::kUnknown;
  }

  std::string getDefaultValue() const {
    return std::to_string(default_value_);
  }

  inline bool isHidden() const {
    return hidden_;
  }

 private:
  const std::string name_;
  const std::string description_;
  const T default_value_;
  const bool hidden_;
};

} // namespace facebook::presto

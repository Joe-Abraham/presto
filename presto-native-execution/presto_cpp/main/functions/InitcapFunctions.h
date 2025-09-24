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

#include "velox/functions/Macros.h"

namespace facebook::presto::functions {

/// initcap(varchar) -> varchar
/// Returns string with the first letter of each word in uppercase and the rest in lowercase.
/// This version uses strictspace=false, treating any non-alphanumeric character as a word boundary.
template <typename T>
struct InitcapFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    result.resize(input.size());
    
    bool isNewWord = true;
    for (size_t i = 0; i < input.size(); ++i) {
      char c = input[i];
      
      if (std::isalnum(c)) {
        if (isNewWord) {
          result[i] = std::toupper(c);
          isNewWord = false;
        } else {
          result[i] = std::tolower(c);
        }
      } else {
        result[i] = c;
        isNewWord = true;  // strictspace=false: any non-alphanumeric starts a new word
      }
    }
  }
};

/// Register initcap functions with the given prefix
void registerInitcapFunctions(const std::string& prefix);

} // namespace facebook::presto::functions
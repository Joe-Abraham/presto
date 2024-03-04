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

#include <list>
#include <memory>
#include <string>

#include "SessionProperty.h"

namespace facebook::presto {

/// Defines all system session properties supported by native worker to ensure
/// that they are the source of truth and to differentiate them from Java based
/// session properties.
class SystemSessionProperties {
 public:
  SystemSessionProperties();

  const std::list<std::unique_ptr<SessionProperty>>& getSessionProperties()
      const;

  // Name of session properties supported by native engine.
  // Enable join spilling on native engine.
  static constexpr const char* kJoinSpillEnabled = "join_spill_enabled";

  // The maximum allowed spilling level for hash join build.
  static constexpr const char* kMaxSpillLevel = "max_spill_level";

  // The maximum size in bytes to buffer the serialized spill data.
  static constexpr const char* kSpillWriteBufferSize =
      "spill_write_buffer_size";

 private:
  std::list<std::unique_ptr<SessionProperty>> session_properties_;
};

} // namespace facebook::presto

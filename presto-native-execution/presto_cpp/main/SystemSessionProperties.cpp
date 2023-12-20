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

#include "SystemSessionProperties.h"

namespace facebook::presto {

SystemSessionProperties::SystemSessionProperties() {
  // Add each SessionPropertyData instances of different types to the list.
  session_properties_.emplace_back(std::make_unique<SessionPropertyData<bool>>(
      kJoinSpillEnabled,
      "Native Execution only. Enable join spilling on native engine",
      false,
      false));

  session_properties_.emplace_back(std::make_unique<SessionPropertyData<int>>(
      kMaxSpillLevel,
      "Native Execution only. The maximum allowed spilling level for hash join build.\n"
      "0 is the initial spilling level, -1 means unlimited.",
      4,
      false));

  session_properties_.emplace_back(std::make_unique<SessionPropertyData<long>>(
      kSpillWriteBufferSize,
      "Native Execution only. The maximum size in bytes to buffer the serialized spill "
      "data before writing to disk for IO efficiency.\nIf set to zero, buffering is disabled.",
      1024 * 1024,
      false));
}

const std::list<std::unique_ptr<SessionProperty>>&
SystemSessionProperties::getSessionProperties() const {
  return session_properties_;
}
} // namespace facebook::presto

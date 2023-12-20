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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/SystemSessionProperties.h"

using namespace facebook::presto;

class SystemSessionPropertiesTest : public testing::Test {};

// Test creation of list of session properties
TEST_F(SystemSessionPropertiesTest, getSystemProperties) {
  SystemSessionProperties system_session_properties;

  EXPECT_EQ(system_session_properties.getSessionProperties().size(), 3);
}

// Validate metadata of the session properties
TEST_F(SystemSessionPropertiesTest, vaildateSessionProperties) {
  SystemSessionProperties system_session_properties;

  for (const auto& property :
       system_session_properties.getSessionProperties()) {
    if (property->getName() == SystemSessionProperties::kJoinSpillEnabled) {
      EXPECT_EQ(property->getType(), PropertyType::kBool);
      EXPECT_THAT(
          property->getDescription(),
          testing::StartsWith(
              "Native Execution only. Enable join spilling on native engine"));
      EXPECT_EQ(property->getDefaultValue(), "0");
      EXPECT_EQ(property->isHidden(), false);
    } else if (property->getName() == SystemSessionProperties::kMaxSpillLevel) {
      EXPECT_EQ(property->getType(), PropertyType::kInt);
      EXPECT_THAT(
          property->getDescription(),
          testing::StartsWith(
              "Native Execution only. The maximum allowed spilling level"));
      EXPECT_EQ(property->getDefaultValue(), "4");
      EXPECT_EQ(property->isHidden(), false);
    } else if (
        property->getName() == SystemSessionProperties::kSpillWriteBufferSize) {
      EXPECT_EQ(property->getType(), PropertyType::kLong);
      EXPECT_THAT(
          property->getDescription(),
          testing::StartsWith(
              "Native Execution only. The maximum size in bytes to buffer"));
      EXPECT_EQ(property->getDefaultValue(), "1048576");
      EXPECT_EQ(property->isHidden(), false);
    } else {
      ASSERT_TRUE(false) << "Invalid property: " << property->getName();
    }
  }
}

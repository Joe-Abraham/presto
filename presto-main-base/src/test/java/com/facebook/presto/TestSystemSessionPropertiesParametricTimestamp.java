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
package com.facebook.presto;

import com.facebook.presto.common.type.TimeZoneKey;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PARAMETRIC_TIMESTAMPS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.isLegacyTimestampEnabled;
import static com.facebook.presto.SystemSessionProperties.isParametricTimestampsEnabled;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSystemSessionPropertiesParametricTimestamp
{
    @Test
    public void testParametricTimestampDefaults()
    {
        Session session = testSessionBuilder().build();

        // Verify conservative defaults for production safety
        assertFalse(isParametricTimestampsEnabled(session));
        assertTrue(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testParametricTimestampEnabled()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "true")
                .build();

        assertTrue(isParametricTimestampsEnabled(session));
        // Legacy should still be true by default
        assertTrue(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testLegacyTimestampDisabled()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "true")
                .setSystemProperty(LEGACY_TIMESTAMP_ENABLED, "false")
                .build();

        assertTrue(isParametricTimestampsEnabled(session));
        assertFalse(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testBothModesEnabled()
    {
        // Test that both modes can be enabled simultaneously for migration
        Session session = testSessionBuilder()
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "true")
                .setSystemProperty(LEGACY_TIMESTAMP_ENABLED, "true")
                .build();

        assertTrue(isParametricTimestampsEnabled(session));
        assertTrue(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testLegacyOnlyMode()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "false")
                .setSystemProperty(LEGACY_TIMESTAMP_ENABLED, "true")
                .build();

        assertFalse(isParametricTimestampsEnabled(session));
        assertTrue(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testParametricOnlyMode()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "true")
                .setSystemProperty(LEGACY_TIMESTAMP_ENABLED, "false")
                .build();

        assertTrue(isParametricTimestampsEnabled(session));
        assertFalse(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testSessionPropertyOverrides()
    {
        // Start with default session
        Session defaultSession = testSessionBuilder().build();
        assertFalse(isParametricTimestampsEnabled(defaultSession));
        assertTrue(isLegacyTimestampEnabled(defaultSession));

        // Override via session properties
        Session overriddenSession = Session.builder(defaultSession)
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "true")
                .setSystemProperty(LEGACY_TIMESTAMP_ENABLED, "false")
                .build();

        assertTrue(isParametricTimestampsEnabled(overriddenSession));
        assertFalse(isLegacyTimestampEnabled(overriddenSession));
    }

    @Test
    public void testSessionWithTimeZone()
    {
        // Test that timestamp configuration works with different time zones
        Session utcSession = testSessionBuilder()
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "true")
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .build();

        assertTrue(isParametricTimestampsEnabled(utcSession));
        assertEquals(utcSession.getTimeZoneKey(), TimeZoneKey.UTC_KEY);

        Session nySession = testSessionBuilder()
                .setSystemProperty(PARAMETRIC_TIMESTAMPS_ENABLED, "true")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/New_York"))
                .build();

        assertTrue(isParametricTimestampsEnabled(nySession));
        assertEquals(nySession.getTimeZoneKey(), TimeZoneKey.getTimeZoneKey("America/New_York"));
    }
}
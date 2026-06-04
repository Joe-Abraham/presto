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

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.isLegacyTimestampEnabled;
import static com.facebook.presto.SystemSessionProperties.isParametricTimestampsEnabled;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for parametric timestamp configuration across system components.
 */
public class TestParametricTimestampConfig
{
    @Test
    public void testProductionSafeDefaults()
    {
        // Test that all components have production-safe defaults
        FeaturesConfig featuresConfig = new FeaturesConfig();
        assertFalse(featuresConfig.isParametricTimestampsEnabled());
        assertTrue(featuresConfig.isLegacyTimestampEnabled());
        assertTrue(featuresConfig.isValidTimestampConfiguration());

        Session session = testSessionBuilder().build();
        assertFalse(isParametricTimestampsEnabled(session));
        assertTrue(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testGradualMigrationScenario()
    {
        // Test gradual migration: legacy enabled, parametric enabled
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setParametricTimestampsEnabled(true);
        featuresConfig.setLegacyTimestampEnabled(true);

        assertTrue(featuresConfig.isParametricTimestampsEnabled());
        assertTrue(featuresConfig.isLegacyTimestampEnabled());
        assertTrue(featuresConfig.isValidTimestampConfiguration());

        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setSystemProperty("deprecated.legacy_timestamp", "true")
                .build();

        assertTrue(isParametricTimestampsEnabled(session));
        assertTrue(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testFullMigrationScenario()
    {
        // Test complete migration: parametric enabled, legacy disabled
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setParametricTimestampsEnabled(true);
        featuresConfig.setLegacyTimestampEnabled(false);

        assertTrue(featuresConfig.isParametricTimestampsEnabled());
        assertFalse(featuresConfig.isLegacyTimestampEnabled());
        assertTrue(featuresConfig.isValidTimestampConfiguration());

        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setSystemProperty("deprecated.legacy_timestamp", "false")
                .build();

        assertTrue(isParametricTimestampsEnabled(session));
        assertFalse(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testRollbackScenario()
    {
        // Test rollback: parametric disabled, legacy enabled
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setParametricTimestampsEnabled(false);
        featuresConfig.setLegacyTimestampEnabled(true);

        assertFalse(featuresConfig.isParametricTimestampsEnabled());
        assertTrue(featuresConfig.isLegacyTimestampEnabled());
        assertTrue(featuresConfig.isValidTimestampConfiguration());

        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "false")
                .setSystemProperty("deprecated.legacy_timestamp", "true")
                .build();

        assertFalse(isParametricTimestampsEnabled(session));
        assertTrue(isLegacyTimestampEnabled(session));
    }

    @Test
    public void testSessionOverrideScenarios()
    {
        // Test session-level overrides for testing/experimentation
        Session baseSession = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "false")
                .build();

        assertFalse(isParametricTimestampsEnabled(baseSession));
        assertTrue(isLegacyTimestampEnabled(baseSession));

        // Override at session level
        Session overriddenSession = Session.builder(baseSession)
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .build();

        assertTrue(isParametricTimestampsEnabled(overriddenSession));
        assertTrue(isLegacyTimestampEnabled(overriddenSession));
    }

    @Test
    public void testFeatureFlagConsistency()
    {
        // Verify that FeaturesConfig and SystemSessionProperties are consistent
        FeaturesConfig config = new FeaturesConfig();

        // Test default state
        Session defaultSession = testSessionBuilder().build();
        assertFalse(isParametricTimestampsEnabled(defaultSession));
        assertTrue(isLegacyTimestampEnabled(defaultSession));

        // Test enabled state
        config.setParametricTimestampsEnabled(true);
        Session enabledSession = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .build();
        assertTrue(isParametricTimestampsEnabled(enabledSession));

        // Test mixed state
        config.setLegacyTimestampEnabled(false);
        Session mixedSession = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setSystemProperty("deprecated.legacy_timestamp", "false")
                .build();
        assertTrue(isParametricTimestampsEnabled(mixedSession));
        assertFalse(isLegacyTimestampEnabled(mixedSession));
    }
}
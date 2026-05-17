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
package com.facebook.presto.sql.analyzer;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFeaturesConfigParametricTimestamp
{
    @Test
    public void testDefaultConfiguration()
    {
        FeaturesConfig config = new FeaturesConfig();

        // Verify production-safe defaults
        assertFalse(config.isParametricTimestampsEnabled());
        assertTrue(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());
    }

    @Test
    public void testParametricTimestampsEnabled()
    {
        FeaturesConfig config = new FeaturesConfig();
        config.setParametricTimestampsEnabled(true);

        assertTrue(config.isParametricTimestampsEnabled());
        assertTrue(config.isLegacyTimestampEnabled()); // Still true by default
        assertTrue(config.isValidTimestampConfiguration());
    }

    @Test
    public void testLegacyTimestampDisabled()
    {
        FeaturesConfig config = new FeaturesConfig();
        config.setParametricTimestampsEnabled(true);
        config.setLegacyTimestampEnabled(false);

        assertTrue(config.isParametricTimestampsEnabled());
        assertFalse(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());
    }

    @Test
    public void testBothModesEnabled()
    {
        FeaturesConfig config = new FeaturesConfig();
        config.setParametricTimestampsEnabled(true);
        config.setLegacyTimestampEnabled(true);

        assertTrue(config.isParametricTimestampsEnabled());
        assertTrue(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());
    }

    @Test
    public void testLegacyOnlyMode()
    {
        FeaturesConfig config = new FeaturesConfig();
        config.setParametricTimestampsEnabled(false);
        config.setLegacyTimestampEnabled(true);

        assertFalse(config.isParametricTimestampsEnabled());
        assertTrue(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());
    }

    @Test
    public void testInvalidConfigurationBothDisabled()
    {
        FeaturesConfig config = new FeaturesConfig();
        config.setParametricTimestampsEnabled(false);
        config.setLegacyTimestampEnabled(false);

        assertFalse(config.isParametricTimestampsEnabled());
        assertFalse(config.isLegacyTimestampEnabled());
        assertFalse(config.isValidTimestampConfiguration());
    }

    @Test
    public void testConfigurationTransitions()
    {
        FeaturesConfig config = new FeaturesConfig();

        // Start with defaults
        assertFalse(config.isParametricTimestampsEnabled());
        assertTrue(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());

        // Enable parametric while keeping legacy
        config.setParametricTimestampsEnabled(true);
        assertTrue(config.isParametricTimestampsEnabled());
        assertTrue(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());

        // Disable legacy while parametric is enabled
        config.setLegacyTimestampEnabled(false);
        assertTrue(config.isParametricTimestampsEnabled());
        assertFalse(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());

        // Re-enable legacy for gradual migration
        config.setLegacyTimestampEnabled(true);
        assertTrue(config.isParametricTimestampsEnabled());
        assertTrue(config.isLegacyTimestampEnabled());
        assertTrue(config.isValidTimestampConfiguration());
    }
}
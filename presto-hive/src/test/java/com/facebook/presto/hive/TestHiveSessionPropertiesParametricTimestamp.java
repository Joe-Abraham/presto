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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveSessionProperties.TIMESTAMP_PRECISION;
import static com.facebook.presto.hive.HiveSessionProperties.getTimestampPrecision;
import static com.facebook.presto.hive.HiveTimestampPrecision.MICROSECONDS;
import static com.facebook.presto.hive.HiveTimestampPrecision.MILLISECONDS;
import static com.facebook.presto.hive.HiveTimestampPrecision.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestHiveSessionPropertiesParametricTimestamp
{
    @Test
    public void testDefaultTimestampPrecision()
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableMap.of());
        assertEquals(getTimestampPrecision(session), MILLISECONDS);
    }
    
    @Test
    public void testTimestampPrecisionMilliseconds()
    {
        ConnectorSession session = new TestingConnectorSession(
            ImmutableMap.of(TIMESTAMP_PRECISION, "MILLISECONDS"));
        assertEquals(getTimestampPrecision(session), MILLISECONDS);
    }
    
    @Test
    public void testTimestampPrecisionMicroseconds()
    {
        ConnectorSession session = new TestingConnectorSession(
            ImmutableMap.of(TIMESTAMP_PRECISION, "MICROSECONDS"));
        assertEquals(getTimestampPrecision(session), MICROSECONDS);
    }
    
    @Test
    public void testTimestampPrecisionNanoseconds()
    {
        ConnectorSession session = new TestingConnectorSession(
            ImmutableMap.of(TIMESTAMP_PRECISION, "NANOSECONDS"));
        assertEquals(getTimestampPrecision(session), NANOSECONDS);
    }
    
    @Test
    public void testTimestampPrecisionCaseInsensitive()
    {
        ConnectorSession session1 = new TestingConnectorSession(
            ImmutableMap.of(TIMESTAMP_PRECISION, "microseconds"));
        assertEquals(getTimestampPrecision(session1), MICROSECONDS);
        
        ConnectorSession session2 = new TestingConnectorSession(
            ImmutableMap.of(TIMESTAMP_PRECISION, "Nanoseconds"));
        assertEquals(getTimestampPrecision(session2), NANOSECONDS);
        
        ConnectorSession session3 = new TestingConnectorSession(
            ImmutableMap.of(TIMESTAMP_PRECISION, "MILLISECONDS"));
        assertEquals(getTimestampPrecision(session3), MILLISECONDS);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidTimestampPrecision()
    {
        new TestingConnectorSession(ImmutableMap.of(TIMESTAMP_PRECISION, "INVALID"));
    }
    
    @Test
    public void testTimestampPrecisionPropertyMetadata()
    {
        // Verify property configuration works as expected
        ConnectorSession defaultSession = new TestingConnectorSession(ImmutableMap.of());
        assertEquals(getTimestampPrecision(defaultSession), MILLISECONDS);
        
        // Test each precision level
        for (HiveTimestampPrecision precision : HiveTimestampPrecision.values()) {
            ConnectorSession session = new TestingConnectorSession(
                ImmutableMap.of(TIMESTAMP_PRECISION, precision.name()));
            assertEquals(getTimestampPrecision(session), precision);
        }
    }
}
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
package com.facebook.presto.common.type;

import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestTimestampTypeCreation
{
    @Test
    public void testAllPrecisionsCreateNonNull()
    {
        for (int precision = 0; precision <= 12; precision++) {
            TimestampType type = createTimestampType(precision);
            assertNotNull(type, "createTimestampType(" + precision + ") returned null");
            assertEquals(type.getPrecision(), precision);
        }
    }

    @Test
    public void testShortPrecisionsReturnShortType()
    {
        for (int precision = 0; precision <= 6; precision++) {
            TimestampType type = createTimestampType(precision);
            assertTrue(type instanceof ShortTimestampType,
                    "precision " + precision + " should produce ShortTimestampType, got " + type.getClass().getSimpleName());
            assertTrue(type.isShort());
        }
    }

    @Test
    public void testLongPrecisionsReturnLongType()
    {
        for (int precision = 7; precision <= 12; precision++) {
            TimestampType type = createTimestampType(precision);
            assertTrue(type instanceof LongTimestampType,
                    "precision " + precision + " should produce LongTimestampType, got " + type.getClass().getSimpleName());
            assertTrue(type.isLong());
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativePrecisionThrows()
    {
        createTimestampType(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTooHighPrecisionThrows()
    {
        createTimestampType(13);
    }

    @Test
    public void testTimestampAliasEqualsMilliPrecision()
    {
        assertEquals(TIMESTAMP, createTimestampType(3));
        assertEquals(TIMESTAMP.getPrecision(), 3);
    }

    @Test
    public void testTimestampMicrosecondsAliasEqualsMicroPrecision()
    {
        assertEquals(TIMESTAMP_MICROSECONDS, createTimestampType(6));
        assertEquals(TIMESTAMP_MICROSECONDS.getPrecision(), 6);
    }
}

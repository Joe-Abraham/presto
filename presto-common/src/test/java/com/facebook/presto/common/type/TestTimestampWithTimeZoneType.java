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

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTimestampWithTimeZoneType
{
    @Test
    public void testShortPrecisionsReturnShortType()
    {
        for (int precision = 0; precision <= 3; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            assertTrue(type instanceof ShortTimestampWithTimeZoneType,
                    "precision " + precision + " should produce ShortTimestampWithTimeZoneType, got " + type.getClass().getSimpleName());
            assertTrue(type.isShort());
            assertEquals(type.getPrecision(), precision);
        }
    }

    @Test
    public void testLongPrecisionsReturnLongType()
    {
        for (int precision = 4; precision <= 9; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            assertTrue(type instanceof LongTimestampWithTimeZoneType,
                    "precision " + precision + " should produce LongTimestampWithTimeZoneType, got " + type.getClass().getSimpleName());
            assertTrue(type.isLong());
            assertEquals(type.getPrecision(), precision);
        }
    }

    @Test
    public void testPrecisionAboveMaxThrows()
    {
        try {
            createTimestampWithTimeZoneType(10);
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("9"),
                    "Exception message should mention the max precision (9), got: " + e.getMessage());
            return;
        }
        throw new AssertionError("Expected IllegalArgumentException for precision 10");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativePrecisionThrows()
    {
        createTimestampWithTimeZoneType(-1);
    }

    @Test
    public void testTimestampWithTimeZoneAliasEqualsMilliPrecision()
    {
        assertEquals(TIMESTAMP_WITH_TIME_ZONE, createTimestampWithTimeZoneType(3));
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getPrecision(), 3);
    }

    @Test
    public void testMaxPrecisionConstantIsNine()
    {
        assertEquals(LongTimestampWithTimeZoneType.MAX_PRECISION_TSTZ, 9);
    }
}

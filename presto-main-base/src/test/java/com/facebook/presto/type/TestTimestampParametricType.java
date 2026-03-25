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
package com.facebook.presto.type;

import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_NANOS;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_SECONDS;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * Tests for the parameterised {@code TIMESTAMP(p)} type covering all supported
 * precisions: 0 (seconds), 3 (milliseconds, default), 6 (microseconds) and 9
 * (nanoseconds).
 */
public class TestTimestampParametricType
        extends AbstractTestFunctions
{
    // -----------------------------------------------------------------------
    // TimestampType.createTimestampType factory
    // -----------------------------------------------------------------------

    @Test
    public void testCreateTimestampTypeMilliseconds()
    {
        TimestampType type = TimestampType.createTimestampType(3);
        assertEquals(type, TIMESTAMP);
        assertEquals(type.getPrecision(), MILLISECONDS);
    }

    @Test
    public void testCreateTimestampTypeMicroseconds()
    {
        TimestampType type = TimestampType.createTimestampType(6);
        assertEquals(type, TIMESTAMP_MICROSECONDS);
        assertEquals(type.getPrecision(), MICROSECONDS);
    }

    @Test
    public void testCreateTimestampTypeSeconds()
    {
        TimestampType type = TimestampType.createTimestampType(0);
        assertEquals(type, TIMESTAMP_SECONDS);
        assertEquals(type.getPrecision(), SECONDS);
    }

    @Test
    public void testCreateTimestampTypeNanoseconds()
    {
        TimestampType type = TimestampType.createTimestampType(9);
        assertEquals(type, TIMESTAMP_NANOS);
        assertEquals(type.getPrecision(), NANOSECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateTimestampTypeInvalidPrecision()
    {
        TimestampType.createTimestampType(7);
    }

    // -----------------------------------------------------------------------
    // Type-signature checks
    // -----------------------------------------------------------------------

    @Test
    public void testTimestampSignature()
    {
        assertEquals(TIMESTAMP.getTypeSignature().toString(), "timestamp");
    }

    @Test
    public void testTimestampMicrosecondsSignature()
    {
        assertEquals(TIMESTAMP_MICROSECONDS.getTypeSignature().toString(), "timestamp(6)");
    }

    @Test
    public void testTimestampSecondsSignature()
    {
        assertEquals(TIMESTAMP_SECONDS.getTypeSignature().toString(), "timestamp(0)");
    }

    @Test
    public void testTimestampNanosSignature()
    {
        assertEquals(TIMESTAMP_NANOS.getTypeSignature().toString(), "timestamp(9)");
    }

    // -----------------------------------------------------------------------
    // Equality / identity
    // -----------------------------------------------------------------------

    @Test
    public void testEquality()
    {
        assertEquals(TIMESTAMP, TimestampType.createTimestampType(3));
        assertEquals(TIMESTAMP_MICROSECONDS, TimestampType.createTimestampType(6));
        assertEquals(TIMESTAMP_SECONDS, TimestampType.createTimestampType(0));
        assertEquals(TIMESTAMP_NANOS, TimestampType.createTimestampType(9));

        assertNotEquals(TIMESTAMP, TIMESTAMP_MICROSECONDS);
        assertNotEquals(TIMESTAMP, TIMESTAMP_SECONDS);
        assertNotEquals(TIMESTAMP, TIMESTAMP_NANOS);
        assertNotEquals(TIMESTAMP_MICROSECONDS, TIMESTAMP_NANOS);
        assertNotEquals(TIMESTAMP_SECONDS, TIMESTAMP_NANOS);
    }

    // -----------------------------------------------------------------------
    // TIMESTAMP(p) literals and precision detection
    // -----------------------------------------------------------------------

    @Test
    public void testLiteralDefaultPrecision()
    {
        // 3 fractional digits → millisecond precision (default TIMESTAMP)
        assertFunction("TIMESTAMP '2023-01-15 12:34:56.123'", TIMESTAMP,
                sqlTimestampOf(2023, 1, 15, 12, 34, 56, 123, session));
    }

    @Test
    public void testLiteralNoFractionalDigits()
    {
        // 0 fractional digits → still millisecond precision (default TIMESTAMP)
        assertFunction("TIMESTAMP '2023-01-15 12:34:56'", TIMESTAMP,
                sqlTimestampOf(2023, 1, 15, 12, 34, 56, 0, session));
    }

    @Test
    public void testLiteralMicrosecondPrecision()
    {
        // 6 fractional digits → TIMESTAMP_MICROSECONDS
        // Epoch itself with microseconds: 0s + 123456µs = 123456µs since epoch.
        // In UTC the string representation must include 6 fractional digits.
        assertFunction(
                "TIMESTAMP '1970-01-01 00:00:00.123456'",
                TIMESTAMP_MICROSECONDS,
                new com.facebook.presto.common.type.SqlTimestamp(123456L, java.util.concurrent.TimeUnit.MICROSECONDS));
    }

    // -----------------------------------------------------------------------
    // Epoch-second / nanosecond accessor helpers
    // -----------------------------------------------------------------------

    @Test
    public void testGetEpochSecond()
    {
        // 1 000 ms from epoch → 1 second
        assertEquals(TIMESTAMP.getEpochSecond(1_000L), 1L);
        // 1 000 000 µs from epoch → 1 second
        assertEquals(TIMESTAMP_MICROSECONDS.getEpochSecond(1_000_000L), 1L);
        // 1 second from epoch
        assertEquals(TIMESTAMP_SECONDS.getEpochSecond(1L), 1L);
        // 1 000 000 000 ns from epoch → 1 second
        assertEquals(TIMESTAMP_NANOS.getEpochSecond(1_000_000_000L), 1L);
    }

    @Test
    public void testGetNanos()
    {
        // 1 500 ms → 500 ms nanosecond portion = 500 000 000 ns
        assertEquals(TIMESTAMP.getNanos(1_500L), 500_000_000);
        // 1 000 500 µs → 500 µs nanosecond portion = 500 000 ns
        assertEquals(TIMESTAMP_MICROSECONDS.getNanos(1_000_500L), 500_000);
    }
}

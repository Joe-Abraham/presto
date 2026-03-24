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

import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteralMicros;
import static com.facebook.presto.util.DateTimeUtils.timestampLiteralPrecision;
import static org.testng.Assert.assertEquals;

/**
 * Tests for the parametric {@code TIMESTAMP(p)} type and related utilities.
 *
 * Uses a non-legacy-timestamp session (same as {@link TestTimestamp}) so that
 * timestamp literals are treated as UTC wall-clock values.
 */
public class TestTimestampPrecision
        extends AbstractTestFunctions
{
    public TestTimestampPrecision()
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .build());
    }

    // -------------------------------------------------------------------------
    // CAST with explicit TIMESTAMP(p) type annotation
    // -------------------------------------------------------------------------

    @Test
    public void testCastToTimestamp3()
    {
        // TIMESTAMP(3) resolves to the millisecond-precision TIMESTAMP type.
        assertFunction(
                "CAST('2001-01-22 03:04:05.321' AS TIMESTAMP(3))",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
    }

    @Test
    public void testCastToTimestamp6()
    {
        // TIMESTAMP(6) resolves to the microsecond-precision TIMESTAMP_MICROSECONDS type.
        long epochMicros = parseTimestampLiteralMicros("2001-01-22 03:04:05.321000");
        assertFunction(
                "CAST('2001-01-22 03:04:05.321' AS TIMESTAMP(6))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(epochMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testCastToTimestamp0()
    {
        // TIMESTAMP(0) resolves to the millisecond-precision TIMESTAMP type (seconds only).
        assertFunction(
                "CAST('2001-01-22 03:04:05' AS TIMESTAMP(0))",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 0)));
    }

    @Test
    public void testCastToTimestamp12()
    {
        // TIMESTAMP(12) resolves to TIMESTAMP_MICROSECONDS (microsecond storage; precision
        // 7-12 is stored with 6 significant fractional digits due to microsecond storage limit).
        long expectedMicros = parseTimestampLiteralMicros("2001-01-22 03:04:05.123456");
        assertFunction(
                "CAST('2001-01-22 03:04:05.123456' AS TIMESTAMP(12))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    // -------------------------------------------------------------------------
    // Timestamp literal precision detection
    // -------------------------------------------------------------------------

    @Test
    public void testTimestampLiteralPrecisionNoFraction()
    {
        assertEquals(timestampLiteralPrecision("2024-01-01 00:00:00"), 0);
    }

    @Test
    public void testTimestampLiteralPrecisionMilliseconds()
    {
        assertEquals(timestampLiteralPrecision("2024-01-01 00:00:00.123"), 3);
    }

    @Test
    public void testTimestampLiteralPrecisionMicroseconds()
    {
        assertEquals(timestampLiteralPrecision("2024-01-01 00:00:00.123456"), 6);
    }

    @Test
    public void testTimestampLiteralPrecisionNanoseconds()
    {
        assertEquals(timestampLiteralPrecision("2024-01-01 00:00:00.123456789"), 9);
    }

    @Test
    public void testTimestampLiteralPrecisionWithTimezone()
    {
        // Timezone offset should not be counted as fractional digits.
        assertEquals(timestampLiteralPrecision("2024-01-01 00:00:00.123 UTC"), 3);
        assertEquals(timestampLiteralPrecision("2024-01-01 00:00:00.123456 +05:30"), 6);
    }

    // -------------------------------------------------------------------------
    // parseTimestampLiteralMicros
    // -------------------------------------------------------------------------

    @Test
    public void testParseTimestampLiteralMicrosEpoch()
    {
        long microseconds = parseTimestampLiteralMicros("1970-01-01 00:00:00");
        assertEquals(microseconds, 0L);
    }

    @Test
    public void testParseTimestampLiteralMicrosMilliseconds()
    {
        // 321 ms -> 321_000 us after the epoch.
        long microseconds = parseTimestampLiteralMicros("1970-01-01 00:00:00.321");
        assertEquals(microseconds, 321_000L);
    }

    @Test
    public void testParseTimestampLiteralMicrosMicroseconds()
    {
        // 321456 us after the epoch.
        long microseconds = parseTimestampLiteralMicros("1970-01-01 00:00:00.321456");
        assertEquals(microseconds, 321_456L);
    }

    @Test
    public void testParseTimestampLiteralMicrosHighPrecisionTruncated()
    {
        // 9-digit nanosecond literal -> truncated to 6 digits (microseconds).
        long microseconds = parseTimestampLiteralMicros("1970-01-01 00:00:00.321456789");
        assertEquals(microseconds, 321_456L);
    }

    @Test
    public void testParseTimestampLiteralMicrosArbitraryDate()
    {
        // 2001-01-22 03:04:05.123456 UTC
        long millisPart = new DateTime(2001, 1, 22, 3, 4, 5, 123, DateTimeZone.UTC).getMillis();
        long expected = TimeUnit.MILLISECONDS.toMicros(millisPart) + 456L;
        long actual = parseTimestampLiteralMicros("2001-01-22 03:04:05.123456");
        assertEquals(actual, expected);
    }

    // -------------------------------------------------------------------------
    // Literal typing: literals with > 3 fractional digits -> TIMESTAMP_MICROSECONDS
    // -------------------------------------------------------------------------

    @Test
    public void testLiteralWithMicrosecondPrecisionEquality()
    {
        // A timestamp literal with 6 fractional digits should be comparable with itself.
        assertFunction(
                "TIMESTAMP '2001-01-22 03:04:05.123456' = TIMESTAMP '2001-01-22 03:04:05.123456'",
                BOOLEAN,
                true);
    }

    @Test
    public void testLiteralWithMillisecondPrecisionEquality()
    {
        // A timestamp literal with 3 fractional digits should be typed as TIMESTAMP (ms).
        assertFunction(
                "TIMESTAMP '2001-01-22 03:04:05.123' = TIMESTAMP '2001-01-22 03:04:05.123'",
                BOOLEAN,
                true);
    }
}

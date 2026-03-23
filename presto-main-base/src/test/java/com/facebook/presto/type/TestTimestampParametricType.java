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
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

/**
 * Integration tests for the parameterized TIMESTAMP(p) type syntax.
 * <p>
 * Covers:
 * <ul>
 *   <li>Type resolution ({@code TIMESTAMP(3)} → milliseconds, {@code TIMESTAMP(6)} → microseconds)</li>
 *   <li>Literal parsing precision (3-digit vs 6-digit fractional seconds)</li>
 *   <li>Display format (3 vs 6 decimal places)</li>
 *   <li>CAST from milliseconds to microseconds</li>
 *   <li>Implicit coercion from {@code TIMESTAMP} to {@code TIMESTAMP_MICROSECONDS}</li>
 *   <li>Error handling for unsupported precisions</li>
 * </ul>
 */
public class TestTimestampParametricType
        extends AbstractTestFunctions
{
    private static final TimeZoneKey SESSION_TIMEZONE = TimeZoneKey.UTC_KEY;

    public TestTimestampParametricType()
    {
        // Use non-legacy timestamp mode so 6-digit literals are correctly typed as
        // TIMESTAMP_MICROSECONDS and parsed at full microsecond precision.
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .setTimeZoneKey(SESSION_TIMEZONE)
                .build());
    }

    // -------------------------------------------------------------------------
    // 1. Type-factory API: TimestampType.createTimestampType(int)
    // -------------------------------------------------------------------------

    @Test
    public void testCreateTimestampType3ReturnsSingleton()
    {
        assertSame(TimestampType.createTimestampType(3), TIMESTAMP);
        assertEquals(TimestampType.createTimestampType(3).getPrecision(), TimeUnit.MILLISECONDS);
    }

    @Test
    public void testCreateTimestampType6ReturnsSingleton()
    {
        assertSame(TimestampType.createTimestampType(6), TIMESTAMP_MICROSECONDS);
        assertEquals(TimestampType.createTimestampType(6).getPrecision(), TimeUnit.MICROSECONDS);
    }

    @Test
    public void testMillisAndMicrosSingletonsAreDifferent()
    {
        assertNotSame(TIMESTAMP, TIMESTAMP_MICROSECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedPrecisionThrows()
    {
        TimestampType.createTimestampType(9);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativePrecisionThrows()
    {
        TimestampType.createTimestampType(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testZeroPrecisionThrows()
    {
        // Only 3 and 6 are supported; 0 is not.
        TimestampType.createTimestampType(0);
    }

    // -------------------------------------------------------------------------
    // 2. Millisecond-precision literals (3 fractional digits → TIMESTAMP)
    // -------------------------------------------------------------------------

    @Test
    public void testMillisecondLiteralTypeInference()
    {
        // A 3-digit fractional-seconds literal must be inferred as TIMESTAMP (ms)
        assertFunction("TIMESTAMP '2024-01-01 10:00:00.123'",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 123_000_000)));
    }

    @Test
    public void testMillisecondLiteralDisplayFormat()
    {
        // Display must always show exactly 3 decimal places
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.123'", TIMESTAMP,
                "2024-01-01 10:00:00.123");
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.000'", TIMESTAMP,
                "2024-01-01 10:00:00.000");
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.500'", TIMESTAMP,
                "2024-01-01 10:00:00.500");
    }

    @Test
    public void testNoFractionalSecondsLiteralIsMilliseconds()
    {
        assertFunction("TIMESTAMP '2024-01-01 10:00:00'",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 0)));
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00'", TIMESTAMP,
                "2024-01-01 10:00:00.000");
    }

    // -------------------------------------------------------------------------
    // 3. Microsecond-precision literals (> 3 fractional digits → TIMESTAMP_MICROSECONDS)
    // -------------------------------------------------------------------------

    @Test
    public void testMicrosecondLiteralTypeInference()
    {
        // A 6-digit fractional-seconds literal must be inferred as TIMESTAMP_MICROSECONDS
        long epochMicros = 1704103200123456L; // 2024-01-01 10:00:00.123456 UTC
        assertFunction("TIMESTAMP '2024-01-01 10:00:00.123456'",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(epochMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testMicrosecondLiteralDisplayFormat()
    {
        // Display must always show exactly 6 decimal places
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.123456'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.123456");
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.000001'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.000001");
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.999999'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.999999");
    }

    @Test
    public void testMicrosecondLiteralSubMillisecondPreserved()
    {
        // The sub-millisecond part (.456 µs) must NOT be truncated
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:01.999999'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:01.999999");
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:02.000001'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:02.000001");
    }

    @Test
    public void testMicrosecondLiteralZeroSubMillis()
    {
        // 6-digit literal with zeros in positions 4-6 is still microsecond type
        assertFunction("TIMESTAMP '2024-01-01 10:00:00.500000'",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(1704103200500000L, TimeUnit.MICROSECONDS));
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.500000'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.500000");
    }

    @Test
    public void testFourDigitFractionalSeconds()
    {
        // 4-digit fractional seconds → TIMESTAMP_MICROSECONDS, trailing zero appended
        assertFunction("TIMESTAMP '2024-01-01 10:00:00.1234'",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(1704103200123400L, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testFiveDigitFractionalSeconds()
    {
        // 5-digit fractional seconds → TIMESTAMP_MICROSECONDS
        assertFunction("TIMESTAMP '2024-01-01 10:00:00.12345'",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(1704103200123450L, TimeUnit.MICROSECONDS));
    }

    // -------------------------------------------------------------------------
    // 4. CAST from TIMESTAMP (ms) to TIMESTAMP_MICROSECONDS (µs)
    // -------------------------------------------------------------------------

    @Test
    public void testCastMillisToMicroseconds()
    {
        // CAST TIMESTAMP → "timestamp microseconds": each millisecond becomes 1000 µs
        assertFunction(
                "CAST(TIMESTAMP '2024-01-01 10:00:00.123' AS \"timestamp microseconds\")",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(1704103200123000L, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testCastMillisToMicrosecondsDisplayFormat()
    {
        // After casting ms→µs the output must show 6 decimal places
        assertFunctionString(
                "CAST(TIMESTAMP '2024-01-01 10:00:00.500' AS \"timestamp microseconds\")",
                TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.500000");
    }

    @Test
    public void testCastMillisToMicrosecondsEpochZero()
    {
        // Unix epoch in milliseconds cast to microseconds
        assertFunctionString(
                "CAST(TIMESTAMP '1970-01-01 00:00:00.000' AS \"timestamp microseconds\")",
                TIMESTAMP_MICROSECONDS,
                "1970-01-01 00:00:00.000000");
    }

    // -------------------------------------------------------------------------
    // 5. Implicit coercion: TIMESTAMP (ms) → TIMESTAMP_MICROSECONDS (µs)
    // -------------------------------------------------------------------------

    @Test
    public void testImplicitCoercionMillisToMicros()
    {
        // COALESCE forces a common super-type: the ms literal is coerced to µs
        assertFunction(
                "COALESCE(TIMESTAMP '2024-01-01 10:00:00.123456', TIMESTAMP '2024-01-01 10:00:00.000')",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(1704103200123456L, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testImplicitCoercionPreservesMilliValue()
    {
        // When a 3-digit literal is widened to µs, its value must be 500000 µs (= 500 ms)
        assertFunctionString(
                "COALESCE(TIMESTAMP '2024-01-01 10:00:00.500000', TIMESTAMP '2024-01-01 10:00:00.000')",
                TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.500000");
    }

    // -------------------------------------------------------------------------
    // 6. Default (unparameterized) TIMESTAMP is millisecond precision
    // -------------------------------------------------------------------------

    @Test
    public void testUnparameterizedTimestampIsMillisecond()
    {
        assertEquals(TIMESTAMP.getPrecision(), TimeUnit.MILLISECONDS);
        assertSame(TimestampType.createTimestampType(TimestampType.DEFAULT_PRECISION), TIMESTAMP);
    }

    @Test
    public void testUnparameterizedTimestampLiteralFormat()
    {
        assertFunctionString("TIMESTAMP '2001-01-22 03:04:05.321'", TIMESTAMP,
                "2001-01-22 03:04:05.321");
        assertFunctionString("TIMESTAMP '2001-01-22 03:04:05.000'", TIMESTAMP,
                "2001-01-22 03:04:05.000");
    }

    // -------------------------------------------------------------------------
    // 7. Epoch-boundary and edge-case timestamps
    // -------------------------------------------------------------------------

    @Test
    public void testEpochTimestampMilliseconds()
    {
        assertFunctionString("TIMESTAMP '1970-01-01 00:00:00.000'", TIMESTAMP,
                "1970-01-01 00:00:00.000");
    }

    @Test
    public void testEpochTimestampMicroseconds()
    {
        assertFunctionString("TIMESTAMP '1970-01-01 00:00:00.000000'", TIMESTAMP_MICROSECONDS,
                "1970-01-01 00:00:00.000000");
    }

    @Test
    public void testPreEpochMicroseconds()
    {
        // Timestamps before Unix epoch have negative µs values
        assertFunctionString("TIMESTAMP '1969-12-31 23:59:59.999999'", TIMESTAMP_MICROSECONDS,
                "1969-12-31 23:59:59.999999");
    }

    // -------------------------------------------------------------------------
    // 8. Error handling
    // -------------------------------------------------------------------------

    @Test
    public void testInvalidTimestampLiteralRejected()
    {
        assertInvalidFunction("TIMESTAMP 'not a timestamp'",
                SemanticErrorCode.INVALID_LITERAL,
                "line 1:1: 'not a timestamp' is not a valid timestamp literal");
    }

    // -------------------------------------------------------------------------
    // 9. SqlTimestamp display helper verification
    // -------------------------------------------------------------------------

    @Test
    public void testSqlTimestampMillisToStringFormat()
    {
        SqlTimestamp ts = new SqlTimestamp(1704103200123L, TimeUnit.MILLISECONDS);
        assertEquals(ts.toString(), "2024-01-01 10:00:00.123");
    }

    @Test
    public void testSqlTimestampMicrosToStringFormat()
    {
        SqlTimestamp ts = new SqlTimestamp(1704103200123456L, TimeUnit.MICROSECONDS);
        assertEquals(ts.toString(), "2024-01-01 10:00:00.123456");
    }

    @Test
    public void testSqlTimestampMicrosWithZeroSubMillisToString()
    {
        SqlTimestamp ts = new SqlTimestamp(1704103200500000L, TimeUnit.MICROSECONDS);
        assertEquals(ts.toString(), "2024-01-01 10:00:00.500000");
    }
}

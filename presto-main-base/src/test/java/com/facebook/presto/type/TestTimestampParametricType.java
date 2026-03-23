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
 * Tests use non-legacy timestamp mode (legacy_timestamp=false) and UTC session timezone so that
 * TIMESTAMP literals with more than 3 fractional-second digits are correctly typed as
 * {@code TIMESTAMP_MICROSECONDS} and round-trip through the engine without losing precision.
 * <p>
 * Covers:
 * <ul>
 *   <li>Type-factory API: {@link TimestampType#createTimestampType(int)}</li>
 *   <li>Millisecond-precision literal typing and display (3 decimal places)</li>
 *   <li>Microsecond-precision literal typing and display (6 decimal places)</li>
 *   <li>CAST from {@code TIMESTAMP} to {@code TIMESTAMP(6)} (ms × 1000 = µs)</li>
 *   <li>Implicit coercion from {@code TIMESTAMP} to {@code TIMESTAMP_MICROSECONDS}</li>
 *   <li>Error handling for unsupported precisions</li>
 * </ul>
 */
public class TestTimestampParametricType
        extends AbstractTestFunctions
{
    public TestTimestampParametricType()
    {
        // Non-legacy mode is required so that >3-digit literals are typed as TIMESTAMP_MICROSECONDS.
        // UTC session timezone ensures literal wall-clock times equal UTC epoch values.
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
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
        assertNotSame(TimestampType.createTimestampType(3), TimestampType.createTimestampType(6));
    }

    @Test
    public void testTimestampPrecisionConstants()
    {
        assertEquals(TimestampType.DEFAULT_PRECISION, 3);
        assertEquals(TimestampType.MICROSECONDS_PRECISION, 6);
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

    // -------------------------------------------------------------------------
    // 2. Millisecond-precision literals (≤ 3 fractional digits → TIMESTAMP)
    // -------------------------------------------------------------------------

    @Test
    public void testMillisecondLiteralTypeInference()
    {
        // 3-digit fractional-seconds literal → TIMESTAMP (millisecond precision)
        assertFunction("TIMESTAMP '2024-01-01 10:00:00.123'",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 123_000_000)));
    }

    @Test
    public void testMillisecondLiteralDisplayFormat()
    {
        // Display must show exactly 3 decimal places
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
        // A literal with no fractional seconds is also millisecond precision
        assertFunction("TIMESTAMP '2024-01-01 10:00:00'",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 0)));
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00'", TIMESTAMP,
                "2024-01-01 10:00:00.000");
    }

    @Test
    public void testUnparameterizedTimestampLiteralFormat()
    {
        // Existing timestamp literals continue to show 3 decimal places
        assertFunctionString("TIMESTAMP '2001-01-22 03:04:05.321'", TIMESTAMP,
                "2001-01-22 03:04:05.321");
        assertFunctionString("TIMESTAMP '2001-01-22 03:04:05.000'", TIMESTAMP,
                "2001-01-22 03:04:05.000");
    }

    // -------------------------------------------------------------------------
    // 3. Microsecond-precision literals (> 3 fractional digits → TIMESTAMP_MICROSECONDS)
    // -------------------------------------------------------------------------

    @Test
    public void testMicrosecondLiteralTypeInference()
    {
        // 6-digit fractional-seconds literal → TIMESTAMP_MICROSECONDS
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.123456'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.123456");
    }

    @Test
    public void testMicrosecondLiteralDisplayFormat()
    {
        // Display must show exactly 6 decimal places
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.000001'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.000001");
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.999999'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.999999");
    }

    @Test
    public void testMicrosecondLiteralSubMillisecondPreserved()
    {
        // Sub-millisecond digits must NOT be truncated or zeroed out
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:01.999999'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:01.999999");
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:02.000001'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:02.000001");
    }

    @Test
    public void testMicrosecondLiteralZeroSubMillis()
    {
        // 6-digit literal with zeros in positions 4–6 is still microsecond type
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.500000'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.500000");
    }

    @Test
    public void testFourDigitFractionalSeconds()
    {
        // 4-digit fractional seconds → TIMESTAMP_MICROSECONDS (padded to 6 with trailing zeros)
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.1234'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.123400");
    }

    @Test
    public void testFiveDigitFractionalSeconds()
    {
        // 5-digit fractional seconds → TIMESTAMP_MICROSECONDS (padded to 6 with a trailing zero)
        assertFunctionString("TIMESTAMP '2024-01-01 10:00:00.12345'", TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.123450");
    }

    // -------------------------------------------------------------------------
    // 4. CAST from TIMESTAMP (ms) to TIMESTAMP(6) / TIMESTAMP_MICROSECONDS (µs)
    //    TIMESTAMP(6) is the parameterized form that resolves to TIMESTAMP_MICROSECONDS.
    // -------------------------------------------------------------------------

    @Test
    public void testCastMillisToTimestamp6()
    {
        // CAST TIMESTAMP → TIMESTAMP(6): each ms becomes 1000 µs
        assertFunctionString(
                "CAST(TIMESTAMP '2024-01-01 10:00:00.123' AS TIMESTAMP(6))",
                TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.123000");
    }

    @Test
    public void testCastMillisToTimestamp6DisplaySixDecimalPlaces()
    {
        // After the cast, display must show 6 decimal places (trailing zeros for ms portion)
        assertFunctionString(
                "CAST(TIMESTAMP '2024-01-01 10:00:00.500' AS TIMESTAMP(6))",
                TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.500000");
    }

    @Test
    public void testCastEpochMillisToTimestamp6()
    {
        // Unix epoch cast to µs precision still displays 6 decimal places
        assertFunctionString(
                "CAST(TIMESTAMP '1970-01-01 00:00:00.000' AS TIMESTAMP(6))",
                TIMESTAMP_MICROSECONDS,
                "1970-01-01 00:00:00.000000");
    }

    // -------------------------------------------------------------------------
    // 5. Implicit coercion: TIMESTAMP (ms) → TIMESTAMP_MICROSECONDS (µs)
    // -------------------------------------------------------------------------

    @Test
    public void testImplicitCoercionMillisToMicros()
    {
        // COALESCE forces a common super-type: the ms literal is implicitly coerced to µs
        assertFunctionString(
                "COALESCE(TIMESTAMP '2024-01-01 10:00:00.123456', TIMESTAMP '2024-01-01 10:00:00.000')",
                TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.123456");
    }

    @Test
    public void testImplicitCoercionPreservesMilliValue()
    {
        // The 3-digit literal is widened to µs: 500 ms → displays as 500000 µs
        assertFunctionString(
                "COALESCE(TIMESTAMP '2024-01-01 10:00:00.500000', TIMESTAMP '2024-01-01 10:00:00.000')",
                TIMESTAMP_MICROSECONDS,
                "2024-01-01 10:00:00.500000");
    }

    // -------------------------------------------------------------------------
    // 6. Type-system properties
    // -------------------------------------------------------------------------

    @Test
    public void testTimestampTypeProperties()
    {
        assertEquals(TIMESTAMP.getPrecision(), TimeUnit.MILLISECONDS);
        assertEquals(TIMESTAMP_MICROSECONDS.getPrecision(), TimeUnit.MICROSECONDS);
        assertSame(TimestampType.createTimestampType(TimestampType.DEFAULT_PRECISION), TIMESTAMP);
        assertSame(TimestampType.createTimestampType(TimestampType.MICROSECONDS_PRECISION), TIMESTAMP_MICROSECONDS);
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
        // A timestamp just before Unix epoch, stored as a negative µs value
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
    // 9. SqlTimestamp object display format (unit tests for the value object)
    // -------------------------------------------------------------------------

    @Test
    public void testSqlTimestampMillisDisplayFormat()
    {
        // 2024-01-01 10:00:00.123 UTC as epoch milliseconds
        SqlTimestamp ts = new SqlTimestamp(1704103200123L, TimeUnit.MILLISECONDS);
        assertEquals(ts.toString(), "2024-01-01 10:00:00.123");
    }

    @Test
    public void testSqlTimestampMicrosDisplayFormat()
    {
        // 2024-01-01 10:00:00.123456 UTC as epoch microseconds
        SqlTimestamp ts = new SqlTimestamp(1704103200123456L, TimeUnit.MICROSECONDS);
        assertEquals(ts.toString(), "2024-01-01 10:00:00.123456");
    }

    @Test
    public void testSqlTimestampMicrosWithZeroSubMillisDisplayFormat()
    {
        // 2024-01-01 10:00:00.500000 UTC – trailing zeros must be preserved in display
        SqlTimestamp ts = new SqlTimestamp(1704103200500000L, TimeUnit.MICROSECONDS);
        assertEquals(ts.toString(), "2024-01-01 10:00:00.500000");
    }

    @Test
    public void testSqlTimestampEpochMicrosDisplayFormat()
    {
        SqlTimestamp ts = new SqlTimestamp(0L, TimeUnit.MICROSECONDS);
        assertEquals(ts.toString(), "1970-01-01 00:00:00.000000");
    }

    @Test
    public void testSqlTimestampPreEpochMicrosDisplayFormat()
    {
        // -1 µs = 1 microsecond before Unix epoch
        SqlTimestamp ts = new SqlTimestamp(-1L, TimeUnit.MICROSECONDS);
        assertEquals(ts.toString(), "1969-12-31 23:59:59.999999");
    }
}

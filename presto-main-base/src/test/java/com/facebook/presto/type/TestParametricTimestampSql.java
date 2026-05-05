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

import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

/**
 * End-to-end SQL-expression tests for parametric TIMESTAMP types.
 *
 * <p>Covers: comparison operators, AT TIME ZONE precision preservation,
 * EXTRACT (including TIMEZONE_HOUR/MINUTE), typeof(), and null semantics.
 * Runs with legacy_timestamp=false so literals are treated as UTC.
 */
public class TestParametricTimestampSql
        extends AbstractTestFunctions
{
    private static final TimeZoneKey UTC_KEY = getTimeZoneKey("UTC");
    private static final DateTimeZone UTC = DateTimeZone.UTC;

    public TestParametricTimestampSql()
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .setTimeZoneKey(UTC_KEY)
                .build());
    }

    // -----------------------------------------------------------------------
    // typeof() — type name verification
    // -----------------------------------------------------------------------

    @Test
    public void testTypeOfTimestamp()
    {
        // Default timestamp literal maps to precision-3 (milliseconds), whose
        // display name is the bare "timestamp" for backward compatibility.
        assertFunctionString("typeof(TIMESTAMP '2021-06-15 10:30:45.123')", VARCHAR, "timestamp");
        assertFunctionString("typeof(TIMESTAMP '2021-06-15 10:30:45')", VARCHAR, "timestamp");
        assertFunctionString("typeof(TIMESTAMP '2021-06-15')", VARCHAR, "timestamp");
    }

    @Test
    public void testTypeOfTimestampWithTimeZone()
    {
        // Embedded-timezone literal is parsed directly as timestamp with time zone (precision 3).
        assertFunctionString(
                "typeof(TIMESTAMP '2021-01-01 00:00:00 +00:00')",
                VARCHAR,
                "timestamp with time zone");
    }

    // -----------------------------------------------------------------------
    // Comparison operators (TimestampParametricOperators dispatch)
    // -----------------------------------------------------------------------

    @Test
    public void testEqual()
    {
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.123' = TIMESTAMP '2021-06-15 10:30:45.123'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.123' = TIMESTAMP '2021-06-15 10:30:45.124'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2021-06-15' = TIMESTAMP '2021-06-15'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15' = TIMESTAMP '2021-06-16'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.123' <> TIMESTAMP '2021-06-15 10:30:45.124'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.123' <> TIMESTAMP '2021-06-15 10:30:45.123'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' < TIMESTAMP '2021-06-15 10:30:45.001'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.001' < TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' < TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' <= TIMESTAMP '2021-06-15 10:30:45.001'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' <= TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.001' <= TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.001' > TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' > TIMESTAMP '2021-06-15 10:30:45.001'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' > TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.001' >= TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' >= TIMESTAMP '2021-06-15 10:30:45.000'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 10:30:45.000' >= TIMESTAMP '2021-06-15 10:30:45.001'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction(
                "TIMESTAMP '2021-06-15 10:30:45.005' BETWEEN TIMESTAMP '2021-06-15 10:30:45.000' AND TIMESTAMP '2021-06-15 10:30:45.999'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-06-15 10:30:45.000' BETWEEN TIMESTAMP '2021-06-15 10:30:45.000' AND TIMESTAMP '2021-06-15 10:30:45.999'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-06-15 10:30:45.999' BETWEEN TIMESTAMP '2021-06-15 10:30:45.000' AND TIMESTAMP '2021-06-15 10:30:45.999'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-06-15 10:30:46.000' BETWEEN TIMESTAMP '2021-06-15 10:30:45.000' AND TIMESTAMP '2021-06-15 10:30:45.999'",
                BOOLEAN, false);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("TIMESTAMP '2021-01-01' IS DISTINCT FROM TIMESTAMP '2021-01-01'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2021-01-01' IS DISTINCT FROM TIMESTAMP '2021-01-02'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-01-01' IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM NULL", BOOLEAN, false);
        assertFunction("NULL IS DISTINCT FROM TIMESTAMP '2021-01-01'", BOOLEAN, true);
    }

    // -----------------------------------------------------------------------
    // EXTRACT fields on timestamp(3)
    // -----------------------------------------------------------------------

    @Test
    public void testExtractYear()
    {
        assertFunction("EXTRACT(YEAR FROM TIMESTAMP '2021-06-15 10:30:45')", BIGINT, 2021L);
        assertFunction("EXTRACT(YEAR FROM TIMESTAMP '1999-12-31 23:59:59')", BIGINT, 1999L);
    }

    @Test
    public void testExtractMonth()
    {
        assertFunction("EXTRACT(MONTH FROM TIMESTAMP '2021-06-15 10:30:45')", BIGINT, 6L);
        assertFunction("EXTRACT(MONTH FROM TIMESTAMP '2021-01-01')", BIGINT, 1L);
        assertFunction("EXTRACT(MONTH FROM TIMESTAMP '2021-12-31')", BIGINT, 12L);
    }

    @Test
    public void testExtractDay()
    {
        assertFunction("EXTRACT(DAY FROM TIMESTAMP '2021-06-15 10:30:45')", BIGINT, 15L);
        assertFunction("EXTRACT(DAY FROM TIMESTAMP '2021-01-01')", BIGINT, 1L);
    }

    @Test
    public void testExtractHour()
    {
        assertFunction("EXTRACT(HOUR FROM TIMESTAMP '2021-06-15 10:30:45')", BIGINT, 10L);
        assertFunction("EXTRACT(HOUR FROM TIMESTAMP '2021-06-15 00:00:00')", BIGINT, 0L);
        assertFunction("EXTRACT(HOUR FROM TIMESTAMP '2021-06-15 23:59:59')", BIGINT, 23L);
    }

    @Test
    public void testExtractMinute()
    {
        assertFunction("EXTRACT(MINUTE FROM TIMESTAMP '2021-06-15 10:30:45')", BIGINT, 30L);
        assertFunction("EXTRACT(MINUTE FROM TIMESTAMP '2021-06-15 10:00:00')", BIGINT, 0L);
    }

    @Test
    public void testExtractSecond()
    {
        assertFunction("EXTRACT(SECOND FROM TIMESTAMP '2021-06-15 10:30:45')", BIGINT, 45L);
        assertFunction("EXTRACT(SECOND FROM TIMESTAMP '2021-06-15 10:30:00')", BIGINT, 0L);
    }

    @Test
    public void testExtractDayOfWeek()
    {
        // 2021-06-15 is a Tuesday (2)
        assertFunction("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2021-06-15 10:30:45')", BIGINT, 2L);
        // 2021-06-13 is a Sunday (7)
        assertFunction("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2021-06-13')", BIGINT, 7L);
    }

    @Test
    public void testExtractQuarter()
    {
        assertFunction("EXTRACT(QUARTER FROM TIMESTAMP '2021-01-15')", BIGINT, 1L);
        assertFunction("EXTRACT(QUARTER FROM TIMESTAMP '2021-04-15')", BIGINT, 2L);
        assertFunction("EXTRACT(QUARTER FROM TIMESTAMP '2021-07-15')", BIGINT, 3L);
        assertFunction("EXTRACT(QUARTER FROM TIMESTAMP '2021-10-15')", BIGINT, 4L);
    }

    // -----------------------------------------------------------------------
    // AT TIME ZONE — precision preservation and value correctness
    // -----------------------------------------------------------------------

    @Test
    public void testAtTimeZoneProducesTimestampWithTimeZone()
    {
        // Embedded-timezone literal is parsed as timestamp with time zone (precision 3).
        // "+00:00" literal: wall clock = epoch, so epochMillis = 2021-06-15T10:30:45.123 UTC.
        long epochMillis = new DateTime(2021, 6, 15, 10, 30, 45, 123, UTC).getMillis();
        assertFunction(
                "TIMESTAMP '2021-06-15 10:30:45.123 +00:00'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(epochMillis, getTimeZoneKey("+00:00")));
    }

    @Test
    public void testAtTimeZonePositiveOffset()
    {
        // Wall clock 05:30:00 in +05:30 == 00:00:00 UTC, same epoch as the original UTC literal.
        long epochMillis = new DateTime(2021, 1, 1, 0, 0, 0, 0, UTC).getMillis();
        assertFunction(
                "TIMESTAMP '2021-01-01 05:30:00.000 +05:30'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(epochMillis, getTimeZoneKey("+05:30")));
    }

    @Test
    public void testAtTimeZoneNegativeOffset()
    {
        // Wall clock 04:00:00 in -08:00 == 12:00:00 UTC, same epoch as the original UTC literal.
        long epochMillis = new DateTime(2021, 1, 1, 12, 0, 0, 0, UTC).getMillis();
        assertFunction(
                "TIMESTAMP '2021-01-01 04:00:00.000 -08:00'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(epochMillis, getTimeZoneKey("-08:00")));
    }

    // -----------------------------------------------------------------------
    // EXTRACT TIMEZONE_HOUR / TIMEZONE_MINUTE from timestamp with time zone
    // (exercises the parametric TimestampWithTimeZoneFunctions)
    // -----------------------------------------------------------------------

    @Test
    public void testExtractTimezoneHourPositiveOffset()
    {
        assertFunction("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-01-01 00:00:00 +05:30')", BIGINT, 5L);
        assertFunction("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-01-01 00:00:00 +11:00')", BIGINT, 11L);
        assertFunction("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-01-01 00:00:00 +00:00')", BIGINT, 0L);
    }

    @Test
    public void testExtractTimezoneHourNegativeOffset()
    {
        assertFunction("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-01-01 00:00:00 -08:00')", BIGINT, -8L);
        assertFunction("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-01-01 00:00:00 -05:30')", BIGINT, -5L);
    }

    @Test
    public void testExtractTimezoneMinutePositiveOffset()
    {
        assertFunction("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2021-01-01 00:00:00 +05:30')", BIGINT, 30L);
        assertFunction("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2021-01-01 00:00:00 +05:00')", BIGINT, 0L);
        assertFunction("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2021-01-01 00:00:00 +00:00')", BIGINT, 0L);
    }

    @Test
    public void testExtractTimezoneMinuteNegativeOffset()
    {
        assertFunction("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2021-01-01 00:00:00 -05:30')", BIGINT, -30L);
        assertFunction("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2021-01-01 00:00:00 -08:00')", BIGINT, 0L);
    }

    // -----------------------------------------------------------------------
    // EXTRACT on timestamp with time zone (year/month/day/hour/minute/second)
    // -----------------------------------------------------------------------

    @Test
    public void testExtractFromTimestampWithTimeZone()
    {
        // 2021-06-15 10:30:45 UTC == 2021-06-15 16:00:45 +05:30; EXTRACT uses wall clock in the embedded zone.
        assertFunction("EXTRACT(YEAR FROM TIMESTAMP '2021-06-15 16:00:45 +05:30')", BIGINT, 2021L);
        assertFunction("EXTRACT(MONTH FROM TIMESTAMP '2021-06-15 16:00:45 +05:30')", BIGINT, 6L);
        assertFunction("EXTRACT(DAY FROM TIMESTAMP '2021-06-15 16:00:45 +05:30')", BIGINT, 15L);
        assertFunction("EXTRACT(HOUR FROM TIMESTAMP '2021-06-15 16:00:45 +05:30')", BIGINT, 16L);
        assertFunction("EXTRACT(MINUTE FROM TIMESTAMP '2021-06-15 16:00:45 +05:30')", BIGINT, 0L);
        assertFunction("EXTRACT(SECOND FROM TIMESTAMP '2021-06-15 16:00:45 +05:30')", BIGINT, 45L);
    }

    // -----------------------------------------------------------------------
    // NULL semantics
    // -----------------------------------------------------------------------

    @Test
    public void testNullComparisons()
    {
        assertFunction("NULL = TIMESTAMP '2021-01-01'", BOOLEAN, null);
        assertFunction("TIMESTAMP '2021-01-01' = NULL", BOOLEAN, null);
        assertFunction("NULL < TIMESTAMP '2021-01-01'", BOOLEAN, null);
        assertFunction("NULL > TIMESTAMP '2021-01-01'", BOOLEAN, null);
    }

    @Test
    public void testNullBetween()
    {
        assertFunction(
                "NULL BETWEEN TIMESTAMP '2021-01-01' AND TIMESTAMP '2021-12-31'",
                BOOLEAN, null);
    }

    // -----------------------------------------------------------------------
    // Timestamp with time zone comparisons
    // -----------------------------------------------------------------------

    @Test
    public void testTimestampWithTimeZoneEquality()
    {
        assertFunction("TIMESTAMP '2021-01-01 12:00:00 +00:00' = TIMESTAMP '2021-01-01 12:00:00 +00:00'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-01-01 12:00:00 +00:00' = TIMESTAMP '2021-01-01 12:00:01 +00:00'", BOOLEAN, false);
        // Same instant in different zones: 12:00 UTC == 17:30 +05:30
        assertFunction("TIMESTAMP '2021-01-01 12:00:00 +00:00' = TIMESTAMP '2021-01-01 17:30:00 +05:30'", BOOLEAN, true);
    }

    @Test
    public void testTimestampWithTimeZoneOrdering()
    {
        assertFunction("TIMESTAMP '2021-01-01 12:00:00 +00:00' < TIMESTAMP '2021-01-01 12:00:01 +00:00'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-01-01 12:00:01 +00:00' > TIMESTAMP '2021-01-01 12:00:00 +00:00'", BOOLEAN, true);
    }
}

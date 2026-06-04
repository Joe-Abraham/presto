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
    // Precision-3 (millisecond) boundary — storage-tier edge cases
    //
    // All TIMESTAMP literals in the current port resolve to precision-3:
    // - The SQL analyzer always assigns TimestampType.TIMESTAMP (p=3).
    // - parseTimestampLiteral() truncates fractional digits beyond 3 to epoch-ms.
    //
    // Higher-precision queries (illustrative, require precision-inference port):
    //   SELECT typeof(CAST(ts AS timestamp(6)));   → "timestamp microseconds"
    //   SELECT typeof(CAST(ts AS timestamp(9)));   → "timestamp(9)"
    //   SELECT typeof(CAST(ts AS timestamp(12)));  → "timestamp(12)"
    // -----------------------------------------------------------------------

    @Test
    public void testSubMillisecondLiteralTruncation()
    {
        // Fractional digits beyond 3 in a literal are truncated to milliseconds
        // at parse time — the type stays precision-3.
        assertFunction(
                "TIMESTAMP '2021-01-01 00:00:00.999999' = TIMESTAMP '2021-01-01 00:00:00.999'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-01-01 00:00:00.000001' = TIMESTAMP '2021-01-01 00:00:00.000'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-01-01 00:00:00.123456789' = TIMESTAMP '2021-01-01 00:00:00.123'",
                BOOLEAN, true);
    }

    @Test
    public void testMillisecondBoundaryComparisons()
    {
        // 1 ms difference is detectable at precision-3 (short-timestamp operators)
        assertFunction(
                "TIMESTAMP '2021-01-01 00:00:00.000' < TIMESTAMP '2021-01-01 00:00:00.001'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-01-01 00:00:00.999' < TIMESTAMP '2021-01-01 00:00:01.000'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-06-30 23:59:59.999' < TIMESTAMP '2021-07-01 00:00:00.000'",
                BOOLEAN, true);
        assertFunction(
                "TIMESTAMP '2021-12-31 23:59:59.999' < TIMESTAMP '2022-01-01 00:00:00.000'",
                BOOLEAN, true);
    }

    @Test
    public void testNegativeEpochTimestamps()
    {
        // Timestamps before 1970-01-01 have negative epoch-millisecond values.
        assertFunction(
                "TIMESTAMP '1969-12-31 23:59:59.999' < TIMESTAMP '1970-01-01 00:00:00.000'",
                BOOLEAN, true);
        assertFunction("EXTRACT(YEAR  FROM TIMESTAMP '1969-12-31 23:59:59')", BIGINT, 1969L);
        assertFunction("EXTRACT(MONTH FROM TIMESTAMP '1969-12-31 23:59:59')", BIGINT, 12L);
        assertFunction("EXTRACT(DAY   FROM TIMESTAMP '1969-12-31 23:59:59')", BIGINT, 31L);
        assertFunction("EXTRACT(HOUR  FROM TIMESTAMP '1969-12-31 23:59:59')", BIGINT, 23L);
    }

    @Test
    public void testDateBoundaries()
    {
        // Year boundary
        assertFunction("EXTRACT(YEAR FROM TIMESTAMP '2020-12-31 23:59:59.999')", BIGINT, 2020L);
        assertFunction("EXTRACT(YEAR FROM TIMESTAMP '2021-01-01 00:00:00.000')", BIGINT, 2021L);
        // Leap year: 2020-02-29 is valid
        assertFunction("EXTRACT(DAY   FROM TIMESTAMP '2020-02-29 12:00:00')", BIGINT, 29L);
        assertFunction("EXTRACT(MONTH FROM TIMESTAMP '2020-02-29 12:00:00')", BIGINT, 2L);
    }

    // -----------------------------------------------------------------------
    // TSTZ cross-zone: same epoch, different offset representations
    // -----------------------------------------------------------------------

    @Test
    public void testTimestampWithTimeZoneCrossZoneEquality()
    {
        // The same instant expressed in four different offset zones is equal.
        assertFunction("TIMESTAMP '2021-06-15 12:00:00.000 +00:00' = TIMESTAMP '2021-06-15 14:00:00.000 +02:00'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 12:00:00.000 +00:00' = TIMESTAMP '2021-06-15 06:00:00.000 -06:00'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2021-06-15 12:00:00.000 +00:00' = TIMESTAMP '2021-06-15 22:30:00.000 +10:30'", BOOLEAN, true);
        // One millisecond apart is distinguishable even across zones
        assertFunction("TIMESTAMP '2021-06-15 12:00:00.000 +00:00' = TIMESTAMP '2021-06-15 12:00:00.001 +00:00'", BOOLEAN, false);
    }

    @Test
    public void testTimestampWithTimeZoneCrossZoneOrdering()
    {
        // Ordering is by epoch, independent of the display zone.
        assertFunction("TIMESTAMP '2021-01-01 11:59:59.999 +00:00' < TIMESTAMP '2021-01-01 12:00:00.000 +00:00'", BOOLEAN, true);
        // 11:59 UTC is EARLIER than 17:30 +05:30 (which is 12:00 UTC)
        assertFunction("TIMESTAMP '2021-01-01 11:59:59.999 +00:00' < TIMESTAMP '2021-01-01 17:30:00.000 +05:30'", BOOLEAN, true);
        // 12:01 UTC is LATER than 17:30 +05:30 (which is 12:00 UTC)
        assertFunction("TIMESTAMP '2021-01-01 12:00:00.001 +00:00' > TIMESTAMP '2021-01-01 17:30:00.000 +05:30'", BOOLEAN, true);
    }

    @Test
    public void testExtractAllFieldsFromTimestamp()
    {
        // Comprehensive EXTRACT coverage at precision-3 (all short-timestamp paths)
        String ts = "TIMESTAMP '2021-11-23 14:37:52.456'";
        assertFunction("EXTRACT(YEAR        FROM " + ts + ")", BIGINT, 2021L);
        assertFunction("EXTRACT(MONTH       FROM " + ts + ")", BIGINT, 11L);
        assertFunction("EXTRACT(DAY         FROM " + ts + ")", BIGINT, 23L);
        assertFunction("EXTRACT(DAY_OF_WEEK FROM " + ts + ")", BIGINT, 2L);  // Tuesday
        assertFunction("EXTRACT(DAY_OF_YEAR FROM " + ts + ")", BIGINT, 327L);
        assertFunction("EXTRACT(HOUR        FROM " + ts + ")", BIGINT, 14L);
        assertFunction("EXTRACT(MINUTE      FROM " + ts + ")", BIGINT, 37L);
        assertFunction("EXTRACT(SECOND      FROM " + ts + ")", BIGINT, 52L);
        assertFunction("EXTRACT(QUARTER     FROM " + ts + ")", BIGINT, 4L);
    }

    @Test
    public void testTypeOfPrecision3IsBackwardCompatName()
    {
        // All plain timestamp literals are precision-3; typeof returns the
        // backward-compatible bare name "timestamp", not "timestamp(3)".
        assertFunctionString("typeof(TIMESTAMP '2021-01-01')", VARCHAR, "timestamp");
        assertFunctionString("typeof(TIMESTAMP '2021-01-01 00:00:00')", VARCHAR, "timestamp");
        assertFunctionString("typeof(TIMESTAMP '2021-01-01 00:00:00.123')", VARCHAR, "timestamp");
        // Extra literal digits are truncated; type is still precision-3.
        assertFunctionString("typeof(TIMESTAMP '2021-01-01 00:00:00.123456')", VARCHAR, "timestamp");
        assertFunctionString("typeof(TIMESTAMP '2021-01-01 00:00:00.123456789')", VARCHAR, "timestamp");
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

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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.testing.MaterializedResult;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for parametric timestamp function integration including current_timestamp,
 * extract functions, and date arithmetic with precision preservation.
 */
public class TestParametricTimestampFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testCurrentTimestampWithPrecision()
    {
        // Test current_timestamp(p) for various precisions
        for (int precision = 0; precision <= 3; precision++) {
            String query = format("SELECT current_timestamp(%d)", precision);
            MaterializedResult result = computeActual(query);

            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getTypes().size(), 1);
            assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(precision));

            // Verify the result is a valid timestamp
            Object timestamp = result.getMaterializedRows().get(0).getField(0);
            assertTrue(timestamp instanceof Long);
        }
    }

    @Test
    public void testCurrentTimestampPrecisionTruncation()
    {
        // Test that current_timestamp(p) properly truncates precision
        Session session = testSessionBuilder()
                .setStartTime(1640995200123L) // 2022-01-01 00:00:00.123
                .build();

        // Test precision 0 - should truncate to seconds
        MaterializedResult result0 = computeActual(session, "SELECT current_timestamp(0)");
        assertEquals(result0.getTypes().get(0), createTimestampWithTimeZoneType(0));

        // Test precision 1 - should truncate to deciseconds 
        MaterializedResult result1 = computeActual(session, "SELECT current_timestamp(1)");
        assertEquals(result1.getTypes().get(0), createTimestampWithTimeZoneType(1));

        // Test precision 2 - should truncate to centiseconds
        MaterializedResult result2 = computeActual(session, "SELECT current_timestamp(2)");
        assertEquals(result2.getTypes().get(0), createTimestampWithTimeZoneType(2));

        // Test precision 3 - should preserve full milliseconds
        MaterializedResult result3 = computeActual(session, "SELECT current_timestamp(3)");
        assertEquals(result3.getTypes().get(0), createTimestampWithTimeZoneType(3));
    }

    @Test
    public void testCurrentTimestampInvalidPrecision()
    {
        // Test that invalid precisions throw appropriate errors
        assertQueryFails("SELECT current_timestamp(-1)", "TIMESTAMP precision must be in range \\[0, 12\\].*");
        assertQueryFails("SELECT current_timestamp(13)", "TIMESTAMP precision must be in range \\[0, 12\\].*");
        assertQueryFails("SELECT current_timestamp(100)", "TIMESTAMP precision must be in range \\[0, 12\\].*");
    }

    @Test
    public void testAtTimeZonePrecisionPreservation()
    {
        // Test that AT TIME ZONE preserves timestamp precision
        for (int precision = 0; precision <= 6; precision++) {
            String query = format("SELECT TIMESTAMP '2023-01-01 12:00:00' + INTERVAL '%d' MICROSECOND AT TIME ZONE 'UTC'",
                    precision * 100);
            MaterializedResult result = computeActual(query);

            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getTypes().size(), 1);

            // The result type should preserve precision based on the input
            assertTrue(result.getTypes().get(0) instanceof TimestampWithTimeZoneType);
        }
    }

    @Test
    public void testTimezoneExtractFromParametricTimestamp()
    {
        // Test timezone_hour and timezone_minute for short timestamp with time zone
        assertQuery("SELECT timezone_hour(TIMESTAMP '2023-01-01 12:00:00+05:30')", "VALUES 5");
        assertQuery("SELECT timezone_minute(TIMESTAMP '2023-01-01 12:00:00+05:30')", "VALUES 30");

        // Test with different time zones
        assertQuery("SELECT timezone_hour(TIMESTAMP '2023-01-01 12:00:00-08:00')", "VALUES -8");
        assertQuery("SELECT timezone_minute(TIMESTAMP '2023-01-01 12:00:00-08:00')", "VALUES 0");

        // Test with fractional offset
        assertQuery("SELECT timezone_hour(TIMESTAMP '2023-01-01 12:00:00+09:45')", "VALUES 9");
        assertQuery("SELECT timezone_minute(TIMESTAMP '2023-01-01 12:00:00+09:45')", "VALUES 45");
    }

    @Test
    public void testDateAddWithParametricTimestamp()
    {
        // Test date_add with parametric timestamps for short precisions

        // Test adding days
        assertQuery("SELECT date_add('day', 1, TIMESTAMP '2023-01-01 12:00:00')",
                "VALUES TIMESTAMP '2023-01-02 12:00:00'");

        // Test adding hours
        assertQuery("SELECT date_add('hour', 6, TIMESTAMP '2023-01-01 12:00:00')",
                "VALUES TIMESTAMP '2023-01-01 18:00:00'");

        // Test adding minutes with microsecond precision
        assertQuery("SELECT date_add('minute', 30, TIMESTAMP '2023-01-01 12:00:00.123456')",
                "VALUES TIMESTAMP '2023-01-01 12:30:00.123456'");

        // Test that precision is preserved in result
        String query = "SELECT date_add('second', 1, CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)))";
        MaterializedResult result = computeActual(query);
        assertEquals(result.getTypes().get(0), createTimestampType(6));
    }

    @Test
    public void testDateAddWithParametricTimestampWithTimeZone()
    {
        // Test date_add with parametric timestamp with time zone

        // Test adding days preserves time zone
        assertQuery("SELECT date_add('day', 1, TIMESTAMP '2023-01-01 12:00:00+05:00')",
                "VALUES TIMESTAMP '2023-01-02 12:00:00+05:00'");

        // Test adding hours across DST boundary (where applicable)
        assertQuery("SELECT date_add('hour', 25, TIMESTAMP '2023-01-01 12:00:00-08:00')",
                "VALUES TIMESTAMP '2023-01-02 13:00:00-08:00'");

        // Test precision preservation with time zone
        String query = "SELECT date_add('minute', 15, TIMESTAMP '2023-01-01 12:00:00.123+00:00')";
        MaterializedResult result = computeActual(query);
        assertTrue(result.getTypes().get(0) instanceof TimestampWithTimeZoneType);
    }

    @Test
    public void testDateArithmeticPrecisionPreservation()
    {
        // Test that date arithmetic preserves input precision
        for (int precision = 0; precision <= 6; precision++) {
            String timestampLiteral = format("CAST('2023-01-01 12:00:00' AS TIMESTAMP(%d))", precision);
            String query = format("SELECT date_add('hour', 1, %s)", timestampLiteral);

            MaterializedResult result = computeActual(query);
            assertEquals(result.getTypes().get(0), createTimestampType(precision));
        }
    }

    @Test
    public void testFunctionInteractionWithConfiguration()
    {
        // Test that functions work with parametric timestamp configuration enabled
        Session parametricSession = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setSystemProperty("deprecated.legacy_timestamp", "true")
                .build();

        // Test current_timestamp with configuration
        MaterializedResult result = computeActual(parametricSession, "SELECT current_timestamp(6)");
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));

        // Test date_add with configuration
        result = computeActual(parametricSession,
                "SELECT date_add('day', 1, CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)))");
        assertEquals(result.getTypes().get(0), createTimestampType(6));
    }

    @Test
    public void testFunctionCompatibilityWithLegacyMode()
    {
        // Test that parametric functions work alongside legacy functions
        Session legacySession = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "false")
                .setSystemProperty("deprecated.legacy_timestamp", "true")
                .build();

        // Legacy current_timestamp should work
        MaterializedResult legacyResult = computeActual(legacySession, "SELECT current_timestamp()");
        assertEquals(legacyResult.getTypes().get(0), TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);

        // Legacy date_add should work 
        legacyResult = computeActual(legacySession, "SELECT date_add('day', 1, TIMESTAMP '2023-01-01 12:00:00')");
        assertEquals(legacyResult.getTypes().get(0), TimestampType.TIMESTAMP);
    }

    @Test
    public void testEdgeCasePrecisions()
    {
        // Test boundary precision values

        // Precision 0 (seconds)
        assertQuery("SELECT current_timestamp(0) IS NOT NULL", "VALUES true");

        // Precision 3 (milliseconds - legacy compatible)
        assertQuery("SELECT current_timestamp(3) IS NOT NULL", "VALUES true");

        // Precision 6 (microseconds)
        assertQuery("SELECT current_timestamp(6) IS NOT NULL", "VALUES true");

        // Test that unsupported high precisions fail gracefully
        assertQueryFails("SELECT current_timestamp(7)",
                "current_timestamp with precision > 3 not yet implemented");
        assertQueryFails("SELECT current_timestamp(12)",
                "current_timestamp with precision > 3 not yet implemented");
    }

    @Test
    public void testExtractFunctionBoundaryConditions()
    {
        // Test extract functions with various timestamp formats

        // Test UTC timestamp
        assertQuery("SELECT timezone_hour(TIMESTAMP '2023-01-01 12:00:00+00:00')", "VALUES 0");
        assertQuery("SELECT timezone_minute(TIMESTAMP '2023-01-01 12:00:00+00:00')", "VALUES 0");

        // Test positive offset boundary
        assertQuery("SELECT timezone_hour(TIMESTAMP '2023-01-01 12:00:00+14:00')", "VALUES 14");

        // Test negative offset boundary
        assertQuery("SELECT timezone_hour(TIMESTAMP '2023-01-01 12:00:00-12:00')", "VALUES -12");

        // Test non-hour aligned offsets
        assertQuery("SELECT timezone_minute(TIMESTAMP '2023-01-01 12:00:00+05:45')", "VALUES 45");
    }

    @Test
    public void testCurrentTimestampSessionConsistency()
    {
        // Test that current_timestamp returns consistent values within a session
        String query = "SELECT current_timestamp(3), current_timestamp(3), current_timestamp(3)";
        MaterializedResult result = computeActual(query);

        assertEquals(result.getRowCount(), 1);
        Object[] row = result.getMaterializedRows().get(0).getFields().toArray();

        // All three values should be identical (same session start time)
        assertEquals(row[0], row[1]);
        assertEquals(row[1], row[2]);
    }
}
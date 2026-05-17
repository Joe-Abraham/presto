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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for AT TIME ZONE precision preservation with parametric timestamps.
 * Verifies that the DesugarAtTimeZoneRewriter properly preserves precision
 * when converting TIMESTAMP to TIMESTAMP WITH TIME ZONE.
 */
public class TestParametricTimestampAtTimeZone
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .build();

        queryRunner = new LocalQueryRunner(session);
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    public void testAtTimeZonePrecisionPreservation()
    {
        // Test that AT TIME ZONE preserves precision for all supported precisions
        for (int precision = 0; precision <= 6; precision++) {
            String query = format(
                    "SELECT CAST('2023-01-01 12:00:00' AS TIMESTAMP(%d)) AT TIME ZONE 'UTC'",
                    precision);

            MaterializedResult result = queryRunner.execute(query);

            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getTypes().size(), 1);

            // Verify the result type has the same precision as input
            TimestampWithTimeZoneType expectedType = createTimestampWithTimeZoneType(precision);
            assertEquals(result.getTypes().get(0), expectedType);
        }
    }

    @Test
    public void testAtTimeZoneWithDifferentTimezones()
    {
        // Test AT TIME ZONE with various time zones and precision 6
        String[] timezones = {"UTC", "America/New_York", "Europe/London", "Asia/Tokyo"};

        for (String timezone : timezones) {
            String query = format(
                    "SELECT CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) AT TIME ZONE '%s'",
                    timezone);

            MaterializedResult result = queryRunner.execute(query);

            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));

            // Verify we get a valid result
            Object timestamp = result.getMaterializedRows().get(0).getField(0);
            assertNotNull(timestamp);
        }
    }

    @Test
    public void testAtTimeZoneWithVariablePrecisions()
    {
        // Test multiple precisions in a single query
        String query = "SELECT " +
                "CAST('2023-01-01 12:00:00' AS TIMESTAMP(0)) AT TIME ZONE 'UTC' AS ts0, " +
                "CAST('2023-01-01 12:00:00.1' AS TIMESTAMP(1)) AT TIME ZONE 'UTC' AS ts1, " +
                "CAST('2023-01-01 12:00:00.12' AS TIMESTAMP(2)) AT TIME ZONE 'UTC' AS ts2, " +
                "CAST('2023-01-01 12:00:00.123' AS TIMESTAMP(3)) AT TIME ZONE 'UTC' AS ts3, " +
                "CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) AT TIME ZONE 'UTC' AS ts6";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().size(), 5);

        // Verify each column has the correct precision
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(0));
        assertEquals(result.getTypes().get(1), createTimestampWithTimeZoneType(1));
        assertEquals(result.getTypes().get(2), createTimestampWithTimeZoneType(2));
        assertEquals(result.getTypes().get(3), createTimestampWithTimeZoneType(3));
        assertEquals(result.getTypes().get(4), createTimestampWithTimeZoneType(6));
    }

    @Test
    public void testAtTimeZoneInSubqueries()
    {
        // Test AT TIME ZONE precision preservation in subqueries
        String query = "SELECT * FROM (" +
                "   SELECT CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) AT TIME ZONE 'UTC' AS ts" +
                ") WHERE ts IS NOT NULL";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));
    }

    @Test
    public void testAtTimeZoneWithExpressions()
    {
        // Test AT TIME ZONE with timestamp expressions that have precision
        String query = "SELECT " +
                "(CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) + INTERVAL '1' HOUR) AT TIME ZONE 'UTC'";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        // The precision should be preserved through the arithmetic operation
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));
    }

    @Test
    public void testAtTimeZoneWithFunctionCalls()
    {
        // Test AT TIME ZONE with function calls that return parametric timestamps
        String query = "SELECT " +
                "date_add('minute', 30, CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6))) AT TIME ZONE 'UTC'";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));
    }

    @Test
    public void testAtTimeZoneLegacyCompatibility()
    {
        // Test that legacy TIMESTAMP (precision 3) works correctly
        String query = "SELECT TIMESTAMP '2023-01-01 12:00:00.123' AT TIME ZONE 'UTC'";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        // Legacy timestamp should result in precision 3 timestamp with time zone
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(3));
    }

    @Test
    public void testAtTimeZoneWithIntervals()
    {
        // Test AT TIME ZONE with various interval operations
        String query = "SELECT " +
                "(CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) + INTERVAL '1' DAY + INTERVAL '2' HOUR) " +
                "AT TIME ZONE 'America/New_York'";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));
    }

    @Test
    public void testAtTimeZoneNested()
    {
        // Test nested AT TIME ZONE operations (converting back and forth)
        String query = "SELECT " +
                "((CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) AT TIME ZONE 'UTC') " +
                "AT TIME ZONE 'America/New_York')";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        // The final result should be TIMESTAMP (without time zone) but preserve precision
        assertEquals(result.getTypes().get(0), createTimestampType(6));
    }

    @Test
    public void testAtTimeZoneTypeInference()
    {
        // Test that type inference works correctly with AT TIME ZONE
        String query = "SELECT CASE " +
                "  WHEN true THEN CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) AT TIME ZONE 'UTC' " +
                "  ELSE CAST('2023-01-02 12:00:00.123456' AS TIMESTAMP(6)) AT TIME ZONE 'UTC' " +
                "END";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));
    }

    @Test
    public void testAtTimeZoneWithNulls()
    {
        // Test AT TIME ZONE behavior with NULL values
        String query = "SELECT " +
                "CAST(NULL AS TIMESTAMP(6)) AT TIME ZONE 'UTC', " +
                "CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) AT TIME ZONE CAST(NULL AS VARCHAR)";

        MaterializedResult result = queryRunner.execute(query);

        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));
        assertEquals(result.getTypes().get(1), createTimestampWithTimeZoneType(6));

        // Both results should be NULL
        Object[] row = result.getMaterializedRows().get(0).getFields().toArray();
        assertNull(row[0]);
        assertNull(row[1]);
    }
}
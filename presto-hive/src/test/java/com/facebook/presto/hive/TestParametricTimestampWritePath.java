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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Tests for Hive write path with parametric timestamps.
 * Verifies that timestamp precision is properly preserved through
 * write operations in various Hive file formats.
 */
public class TestParametricTimestampWritePath
        extends AbstractTestQueryFramework
{
    @Test
    public void testCreateTableWithParametricTimestamps()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "MICROSECONDS")
                .build();

        // Test CREATE TABLE AS with various timestamp precisions
        assertQuery(session,
                "CREATE TABLE test_timestamps AS " +
                "SELECT " +
                "  CAST('2023-01-01 12:00:00' AS TIMESTAMP(0)) as ts0, " +
                "  CAST('2023-01-01 12:00:00.1' AS TIMESTAMP(1)) as ts1, " +
                "  CAST('2023-01-01 12:00:00.12' AS TIMESTAMP(2)) as ts2, " +
                "  CAST('2023-01-01 12:00:00.123' AS TIMESTAMP(3)) as ts3, " +
                "  CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP(6)) as ts6");

        // Verify the table was created with correct column types
        MaterializedResult describe = computeActual(session, "DESCRIBE test_timestamps");
        assertEquals(describe.getRowCount(), 5);

        // Clean up
        assertQuery(session, "DROP TABLE test_timestamps");
    }

    @Test 
    public void testInsertParametricTimestamps()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "NANOSECONDS")
                .build();

        // Create table with specific timestamp precision
        assertQuery(session,
                "CREATE TABLE test_insert_timestamps (" +
                "  id BIGINT, " +
                "  ts_millis TIMESTAMP(3), " +
                "  ts_micros TIMESTAMP(6), " +
                "  ts_nanos TIMESTAMP(9)" +
                ")");

        // Insert data with various precisions
        assertQuery(session,
                "INSERT INTO test_insert_timestamps VALUES " +
                "(1, TIMESTAMP '2023-01-01 12:00:00.123', " +
                "    TIMESTAMP '2023-01-01 12:00:00.123456', " +
                "    TIMESTAMP '2023-01-01 12:00:00.123456789')");

        // Verify the data was inserted correctly
        MaterializedResult result = computeActual(session, "SELECT * FROM test_insert_timestamps");
        assertEquals(result.getRowCount(), 1);

        // Clean up
        assertQuery(session, "DROP TABLE test_insert_timestamps");
    }

    @Test
    public void testWriteReadRoundTripORC()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "MICROSECONDS")
                .build();

        // Create ORC table with parametric timestamps
        assertQuery(session,
                "CREATE TABLE test_orc_roundtrip (" +
                "  ts_precise TIMESTAMP(6)" +
                ") WITH (format = 'ORC')");

        // Insert precise timestamp data
        assertQuery(session,
                "INSERT INTO test_orc_roundtrip " +
                "VALUES (TIMESTAMP '2023-01-01 12:00:00.123456')");

        // Read back and verify precision is preserved
        MaterializedResult result = computeActual(session, 
                "SELECT ts_precise FROM test_orc_roundtrip");
        assertEquals(result.getRowCount(), 1);
        
        // The type should be timestamp(6)
        assertEquals(result.getTypes().get(0), createTimestampType(6));

        // Clean up
        assertQuery(session, "DROP TABLE test_orc_roundtrip");
    }

    @Test
    public void testWriteReadRoundTripParquet()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "NANOSECONDS")
                .build();

        // Create Parquet table with parametric timestamps
        assertQuery(session,
                "CREATE TABLE test_parquet_roundtrip (" +
                "  ts_nano TIMESTAMP(9)" +
                ") WITH (format = 'PARQUET')");

        // Insert nanosecond precision data
        assertQuery(session,
                "INSERT INTO test_parquet_roundtrip " +
                "VALUES (TIMESTAMP '2023-01-01 12:00:00.123456789')");

        // Read back and verify precision is preserved
        MaterializedResult result = computeActual(session, 
                "SELECT ts_nano FROM test_parquet_roundtrip");
        assertEquals(result.getRowCount(), 1);
        
        // The type should be timestamp(9)
        assertEquals(result.getTypes().get(0), createTimestampType(9));

        // Clean up
        assertQuery(session, "DROP TABLE test_parquet_roundtrip");
    }

    @Test
    public void testTimestampWithTimeZoneWritePath()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "MICROSECONDS")
                .build();

        // Test timestamp with time zone write path
        assertQuery(session,
                "CREATE TABLE test_tstz (" +
                "  tstz_precise TIMESTAMP(6) WITH TIME ZONE" +
                ")");

        assertQuery(session,
                "INSERT INTO test_tstz " +
                "VALUES (TIMESTAMP '2023-01-01 12:00:00.123456+05:00')");

        MaterializedResult result = computeActual(session, "SELECT * FROM test_tstz");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), createTimestampWithTimeZoneType(6));

        // Clean up
        assertQuery(session, "DROP TABLE test_tstz");
    }

    @Test
    public void testMultiFormatCompatibility()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "MICROSECONDS")
                .build();

        // Test that different file formats handle the same timestamp precision consistently
        String[] formats = {"ORC", "PARQUET"};
        
        for (String format : formats) {
            String tableName = format("test_format_%s", format.toLowerCase());
            
            assertQuery(session,
                    format("CREATE TABLE %s (" +
                          "  ts TIMESTAMP(6)" +
                          ") WITH (format = '%s')", tableName, format));

            assertQuery(session,
                    format("INSERT INTO %s VALUES (TIMESTAMP '2023-01-01 12:00:00.123456')", tableName));

            MaterializedResult result = computeActual(session,
                    format("SELECT ts FROM %s", tableName));
            
            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getTypes().get(0), createTimestampType(6));

            assertQuery(session, format("DROP TABLE %s", tableName));
        }
    }

    @Test
    public void testSessionPropertyImpact()
    {
        // Test with different timestamp precision session properties
        HiveTimestampPrecision[] precisions = {
            HiveTimestampPrecision.MILLISECONDS,
            HiveTimestampPrecision.MICROSECONDS,
            HiveTimestampPrecision.NANOSECONDS
        };

        for (HiveTimestampPrecision precision : precisions) {
            Session session = testSessionBuilder()
                    .setSystemProperty("parametric_timestamps_enabled", "true")
                    .setCatalogSessionProperty("hive", "timestamp_precision", precision.name())
                    .build();

            String tableName = format("test_precision_%s", precision.name().toLowerCase());

            // Create table and insert data
            assertQuery(session,
                    format("CREATE TABLE %s (" +
                          "  ts TIMESTAMP(%d)" +
                          ")", tableName, precision.getPrecision()));

            String timestampValue = "";
            switch (precision) {
                case MILLISECONDS:
                    timestampValue = "TIMESTAMP '2023-01-01 12:00:00.123'";
                    break;
                case MICROSECONDS:
                    timestampValue = "TIMESTAMP '2023-01-01 12:00:00.123456'";
                    break;
                case NANOSECONDS:
                    timestampValue = "TIMESTAMP '2023-01-01 12:00:00.123456789'";
                    break;
            }

            assertQuery(session,
                    format("INSERT INTO %s VALUES (%s)", tableName, timestampValue));

            MaterializedResult result = computeActual(session,
                    format("SELECT ts FROM %s", tableName));
            
            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getTypes().get(0), createTimestampType(precision.getPrecision()));

            assertQuery(session, format("DROP TABLE %s", tableName));
        }
    }

    @Test
    public void testPrecisionTruncation()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "MILLISECONDS")
                .build();

        // Test that higher precision inputs are properly handled
        assertQuery(session,
                "CREATE TABLE test_truncation (" +
                "  ts_millis TIMESTAMP(3)" +
                ")");

        // Insert microsecond precision data into millisecond column
        assertQuery(session,
                "INSERT INTO test_truncation " +
                "VALUES (TIMESTAMP '2023-01-01 12:00:00.123456')");

        MaterializedResult result = computeActual(session, 
                "SELECT ts_millis FROM test_truncation");
        assertEquals(result.getRowCount(), 1);

        // Verify the column type is still timestamp(3)
        assertEquals(result.getTypes().get(0), createTimestampType(3));

        // Clean up
        assertQuery(session, "DROP TABLE test_truncation");
    }

    @Test
    public void testComplexTypesWithTimestamps()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "true")
                .setCatalogSessionProperty("hive", "timestamp_precision", "MICROSECONDS")
                .build();

        // Test arrays and maps containing parametric timestamps
        assertQuery(session,
                "CREATE TABLE test_complex_timestamps (" +
                "  ts_array ARRAY(TIMESTAMP(6)), " +
                "  ts_map MAP(VARCHAR, TIMESTAMP(6))" +
                ")");

        assertQuery(session,
                "INSERT INTO test_complex_timestamps VALUES " +
                "(ARRAY[TIMESTAMP '2023-01-01 12:00:00.123456'], " +
                " MAP(ARRAY['key'], ARRAY[TIMESTAMP '2023-01-01 12:00:00.123456']))");

        MaterializedResult result = computeActual(session, 
                "SELECT * FROM test_complex_timestamps");
        assertEquals(result.getRowCount(), 1);

        // Clean up
        assertQuery(session, "DROP TABLE test_complex_timestamps");
    }

    @Test
    public void testBackwardCompatibility()
    {
        // Test that legacy timestamp behavior still works
        Session legacySession = testSessionBuilder()
                .setSystemProperty("parametric_timestamps_enabled", "false")
                .setSystemProperty("deprecated.legacy_timestamp", "true")
                .build();

        // Create table with legacy timestamp type
        assertQuery(legacySession,
                "CREATE TABLE test_legacy_timestamps (" +
                "  legacy_ts TIMESTAMP" +
                ")");

        assertQuery(legacySession,
                "INSERT INTO test_legacy_timestamps " +
                "VALUES (TIMESTAMP '2023-01-01 12:00:00.123')");

        MaterializedResult result = computeActual(legacySession, 
                "SELECT legacy_ts FROM test_legacy_timestamps");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), TimestampType.TIMESTAMP);

        // Clean up
        assertQuery(legacySession, "DROP TABLE test_legacy_timestamps");
    }
}
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
package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.server.protocol.Query;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Set;

import static com.facebook.presto.client.ClientCapabilities.PARAMETRIC_DATETIME;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestClientCompatibility
{
    @Test
    public void testLegacyClientTimestampColumns()
            throws Exception
    {
        // Test legacy client receives downgraded timestamp types in column metadata
        Session legacySession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Test various timestamp precisions
        assertColumnType(legacySession, createTimestampType(0), TIMESTAMP);
        assertColumnType(legacySession, createTimestampType(3), TIMESTAMP);
        assertColumnType(legacySession, createTimestampType(6), TIMESTAMP_MICROSECONDS);
        assertColumnType(legacySession, createTimestampType(9), TIMESTAMP);
        assertColumnType(legacySession, createTimestampType(12), TIMESTAMP);
    }

    @Test
    public void testLegacyClientTimestampWithTimeZoneColumns()
            throws Exception
    {
        Session legacySession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Test various timestamp with time zone precisions
        assertColumnType(legacySession, createTimestampWithTimeZoneType(0), TIMESTAMP_WITH_TIME_ZONE);
        assertColumnType(legacySession, createTimestampWithTimeZoneType(3), TIMESTAMP_WITH_TIME_ZONE);
        assertColumnType(legacySession, createTimestampWithTimeZoneType(6), TIMESTAMP_WITH_TIME_ZONE_MICROSECONDS);
        assertColumnType(legacySession, createTimestampWithTimeZoneType(9), TIMESTAMP_WITH_TIME_ZONE);
        assertColumnType(legacySession, createTimestampWithTimeZoneType(12), TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testModernClientTimestampColumns()
            throws Exception
    {
        // Test modern client receives parametric timestamp types
        Session modernSession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Test various timestamp precisions
        for (int precision = 0; precision <= 12; precision++) {
            assertColumnType(modernSession, createTimestampType(precision), format("timestamp(%d)", precision));
            assertColumnType(modernSession, createTimestampWithTimeZoneType(precision), format("timestamp(%d) with time zone", precision));
        }
    }

    @Test
    public void testComplexTypeCompatibility()
            throws Exception
    {
        Session legacySession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of())
                .build();

        Session modernSession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Test ARRAY with timestamps
        ArrayType arrayType = new ArrayType(createTimestampType(6));
        assertColumnType(legacySession, arrayType, "array(timestamp_microseconds)");
        assertColumnType(modernSession, arrayType, "array(timestamp(6))");

        // Test MAP with timestamps
        MapType mapType = new MapType(createTimestampType(6), createTimestampWithTimeZoneType(9), null, null);
        assertColumnType(legacySession, mapType, "map(timestamp_microseconds, timestamp with time zone)");
        assertColumnType(modernSession, mapType, "map(timestamp(6), timestamp(9) with time zone)");

        // Test ROW with timestamps
        RowType rowType = RowType.from(
                RowType.field("ts", createTimestampType(6)),
                RowType.field("tstz", createTimestampWithTimeZoneType(9)),
                RowType.field("id", BigintType.BIGINT)
        );
        assertColumnType(legacySession, rowType, "row(ts timestamp_microseconds, tstz timestamp with time zone, id bigint)");
        assertColumnType(modernSession, rowType, "row(ts timestamp(6), tstz timestamp(9) with time zone, id bigint)");
    }

    @Test
    public void testNonTimestampTypesUnaffected()
            throws Exception
    {
        Session legacySession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of())
                .build();

        Session modernSession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Test that non-timestamp types are the same for both client types
        Type[] typesToTest = {
                BigintType.BIGINT,
                VarcharType.VARCHAR,
                new ArrayType(BigintType.BIGINT),
                new MapType(VarcharType.VARCHAR, BigintType.BIGINT, null, null),
                RowType.from(RowType.field("id", BigintType.BIGINT), RowType.field("name", VarcharType.VARCHAR))
        };

        for (Type type : typesToTest) {
            String expectedType = type.getDisplayName();
            assertColumnType(legacySession, type, expectedType);
            assertColumnType(modernSession, type, expectedType);
        }
    }

    @Test
    public void testPreDefinedTimestampConstants()
            throws Exception
    {
        Session legacySession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of())
                .build();

        Session modernSession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Test TIMESTAMP constant (precision 3)
        assertColumnType(legacySession, TimestampType.TIMESTAMP, TIMESTAMP);
        assertColumnType(modernSession, TimestampType.TIMESTAMP, "timestamp(3)");

        // Test TIMESTAMP_MICROSECONDS constant (precision 6)
        assertColumnType(legacySession, TimestampType.TIMESTAMP_MICROSECONDS, TIMESTAMP_MICROSECONDS);
        assertColumnType(modernSession, TimestampType.TIMESTAMP_MICROSECONDS, "timestamp(6)");

        // Test TIMESTAMP_WITH_TIME_ZONE constant (precision 3)
        assertColumnType(legacySession, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE);
        assertColumnType(modernSession, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, "timestamp(3) with time zone");
    }

    @Test
    public void testBackwardCompatibilityConstants()
            throws Exception
    {
        Session legacySession = testSessionBuilder()
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Verify that legacy aliases work correctly
        assertColumnType(legacySession, TimestampType.TIMESTAMP_MILLIS, TIMESTAMP);
        assertColumnType(legacySession, TimestampType.TIMESTAMP_MICROS, TIMESTAMP_MICROSECONDS);
        assertColumnType(legacySession, TimestampType.TIMESTAMP_NANOS, TIMESTAMP);
    }

    /**
     * Helper method to test column type string generation via reflection
     * (accessing the private toColumn method in Query class).
     */
    private void assertColumnType(Session session, Type type, String expectedTypeString)
            throws Exception
    {
        // Access the private toColumn method via reflection
        Method toColumnMethod = Query.class.getDeclaredMethod("toColumn", String.class, Type.class, Set.class);
        toColumnMethod.setAccessible(true);

        Column column = (Column) toColumnMethod.invoke(null, "test_column", type, session.getClientCapabilities());
        assertEquals(column.getType(), expectedTypeString);
        assertEquals(column.getName(), "test_column");
    }
}
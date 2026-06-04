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

import com.facebook.presto.client.ClientCapabilities;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.client.ClientCapabilities.PARAMETRIC_DATETIME;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static com.facebook.presto.type.TimestampTypeCompatibility.getTypeStringForClient;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestTimestampTypeCompatibility
{
    private static final Set<ClientCapabilities> LEGACY_CAPABILITIES = ImmutableSet.of();
    private static final Set<ClientCapabilities> MODERN_CAPABILITIES = ImmutableSet.of(PARAMETRIC_DATETIME);

    @Test
    public void testBasicTimestampCompatibility()
    {
        // Test precision 0-12 downgrade logic
        for (int precision = 0; precision <= 12; precision++) {
            TimestampType type = createTimestampType(precision);

            // Modern clients get parametric types
            String modernType = getTypeStringForClient(type, MODERN_CAPABILITIES);
            assertEquals(modernType, format("timestamp(%d)", precision));

            // Legacy clients get downgraded types
            String legacyType = getTypeStringForClient(type, LEGACY_CAPABILITIES);
            if (precision == 6) {
                assertEquals(legacyType, TIMESTAMP_MICROSECONDS);
            }
            else {
                assertEquals(legacyType, TIMESTAMP);
            }
        }
    }

    @Test
    public void testTimestampWithTimeZoneCompatibility()
    {
        // Test timestamp with time zone downgrade
        for (int precision = 0; precision <= 12; precision++) {
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);

            // Modern clients get parametric types
            String modernType = getTypeStringForClient(type, MODERN_CAPABILITIES);
            assertEquals(modernType, format("timestamp(%d) with time zone", precision));

            // Legacy clients get downgraded types
            String legacyType = getTypeStringForClient(type, LEGACY_CAPABILITIES);
            if (precision == 6) {
                assertEquals(legacyType, TIMESTAMP_WITH_TIME_ZONE_MICROSECONDS);
            }
            else {
                assertEquals(legacyType, TIMESTAMP_WITH_TIME_ZONE);
            }
        }
    }

    @Test
    public void testArrayTypeCompatibility()
    {
        // Test ARRAY with parametric timestamps - matches Trino PR #4475
        TimestampType timestampType6 = createTimestampType(6);
        TimestampType timestampType9 = createTimestampType(9);

        ArrayType arrayType6 = new ArrayType(timestampType6);
        ArrayType arrayType9 = new ArrayType(timestampType9);

        // Modern clients
        assertEquals(getTypeStringForClient(arrayType6, MODERN_CAPABILITIES), "array(timestamp(6))");
        assertEquals(getTypeStringForClient(arrayType9, MODERN_CAPABILITIES), "array(timestamp(9))");

        // Legacy clients
        assertEquals(getTypeStringForClient(arrayType6, LEGACY_CAPABILITIES), "array(timestamp_microseconds)");
        assertEquals(getTypeStringForClient(arrayType9, LEGACY_CAPABILITIES), "array(timestamp)");
    }

    @Test
    public void testMapTypeCompatibility()
    {
        // Test MAP with parametric timestamps
        TimestampType keyType = createTimestampType(6);
        TimestampWithTimeZoneType valueType = createTimestampWithTimeZoneType(9);

        MapType mapType = new MapType(keyType, valueType, null, null);

        // Modern clients
        String expectedModern = "map(timestamp(6), timestamp(9) with time zone)";
        assertEquals(getTypeStringForClient(mapType, MODERN_CAPABILITIES), expectedModern);

        // Legacy clients
        String expectedLegacy = "map(timestamp_microseconds, timestamp with time zone)";
        assertEquals(getTypeStringForClient(mapType, LEGACY_CAPABILITIES), expectedLegacy);
    }

    @Test
    public void testRowTypeCompatibility()
    {
        // Test ROW with parametric timestamps
        RowType.Field field1 = RowType.field("ts", createTimestampType(6));
        RowType.Field field2 = RowType.field("tstz", createTimestampWithTimeZoneType(9));
        RowType.Field field3 = RowType.field("id", BigintType.BIGINT);

        RowType rowType = RowType.from(field1, field2, field3);

        // Modern clients
        String expectedModern = "row(ts timestamp(6), tstz timestamp(9) with time zone, id bigint)";
        assertEquals(getTypeStringForClient(rowType, MODERN_CAPABILITIES), expectedModern);

        // Legacy clients
        String expectedLegacy = "row(ts timestamp_microseconds, tstz timestamp with time zone, id bigint)";
        assertEquals(getTypeStringForClient(rowType, LEGACY_CAPABILITIES), expectedLegacy);
    }

    @Test
    public void testAnonymousRowTypeCompatibility()
    {
        // Test ROW without field names
        RowType.Field field1 = RowType.field(createTimestampType(6));
        RowType.Field field2 = RowType.field(VarcharType.VARCHAR);

        RowType rowType = RowType.from(field1, field2);

        // Modern clients
        String expectedModern = "row(timestamp(6), varchar)";
        assertEquals(getTypeStringForClient(rowType, MODERN_CAPABILITIES), expectedModern);

        // Legacy clients
        String expectedLegacy = "row(timestamp_microseconds, varchar)";
        assertEquals(getTypeStringForClient(rowType, LEGACY_CAPABILITIES), expectedLegacy);
    }

    @Test
    public void testComplexNestedTypeCompatibility()
    {
        // Test deeply nested complex type with parametric timestamps
        TimestampType innerType = createTimestampType(9);
        ArrayType arrayOfTimestamps = new ArrayType(innerType);
        MapType mapType = new MapType(VarcharType.VARCHAR, arrayOfTimestamps, null, null);

        RowType.Field field1 = RowType.field("nested_data", mapType);
        RowType.Field field2 = RowType.field("simple_ts", createTimestampType(6));
        RowType complexType = RowType.from(field1, field2);

        // Modern clients
        String expectedModern = "row(nested_data map(varchar, array(timestamp(9))), simple_ts timestamp(6))";
        assertEquals(getTypeStringForClient(complexType, MODERN_CAPABILITIES), expectedModern);

        // Legacy clients
        String expectedLegacy = "row(nested_data map(varchar, array(timestamp)), simple_ts timestamp_microseconds)";
        assertEquals(getTypeStringForClient(complexType, LEGACY_CAPABILITIES), expectedLegacy);
    }

    @Test
    public void testNonTimestampTypesUnchanged()
    {
        // Test that non-timestamp types are unaffected by client capabilities
        Type[] typesToTest = {
                BigintType.BIGINT,
                VarcharType.VARCHAR,
                new ArrayType(BigintType.BIGINT),
                new MapType(VarcharType.VARCHAR, BigintType.BIGINT, null, null),
                RowType.from(RowType.field("id", BigintType.BIGINT), RowType.field("name", VarcharType.VARCHAR))
        };

        for (Type type : typesToTest) {
            String modernType = getTypeStringForClient(type, MODERN_CAPABILITIES);
            String legacyType = getTypeStringForClient(type, LEGACY_CAPABILITIES);
            assertEquals(modernType, legacyType);
            assertEquals(modernType, type.getDisplayName());
        }
    }

    @Test
    public void testPreDefinedTimestampTypes()
    {
        // Test pre-defined timestamp type constants
        assertEquals(getTypeStringForClient(TimestampType.TIMESTAMP, MODERN_CAPABILITIES), "timestamp(3)");
        assertEquals(getTypeStringForClient(TimestampType.TIMESTAMP, LEGACY_CAPABILITIES), TIMESTAMP);

        assertEquals(getTypeStringForClient(TimestampType.TIMESTAMP_MICROSECONDS, MODERN_CAPABILITIES), "timestamp(6)");
        assertEquals(getTypeStringForClient(TimestampType.TIMESTAMP_MICROSECONDS, LEGACY_CAPABILITIES), TIMESTAMP_MICROSECONDS);

        assertEquals(getTypeStringForClient(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, MODERN_CAPABILITIES), "timestamp(3) with time zone");
        assertEquals(getTypeStringForClient(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, LEGACY_CAPABILITIES), TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testEdgeCasePrecisions()
    {
        // Test boundary precisions specifically
        TimestampType precision0 = createTimestampType(0);
        TimestampType precision3 = createTimestampType(3);
        TimestampType precision6 = createTimestampType(6);
        TimestampType precision9 = createTimestampType(9);
        TimestampType precision12 = createTimestampType(12);

        // Modern clients should get exact precision
        assertEquals(getTypeStringForClient(precision0, MODERN_CAPABILITIES), "timestamp(0)");
        assertEquals(getTypeStringForClient(precision3, MODERN_CAPABILITIES), "timestamp(3)");
        assertEquals(getTypeStringForClient(precision6, MODERN_CAPABILITIES), "timestamp(6)");
        assertEquals(getTypeStringForClient(precision9, MODERN_CAPABILITIES), "timestamp(9)");
        assertEquals(getTypeStringForClient(precision12, MODERN_CAPABILITIES), "timestamp(12)");

        // Legacy clients should get downgraded versions
        assertEquals(getTypeStringForClient(precision0, LEGACY_CAPABILITIES), TIMESTAMP);
        assertEquals(getTypeStringForClient(precision3, LEGACY_CAPABILITIES), TIMESTAMP);
        assertEquals(getTypeStringForClient(precision6, LEGACY_CAPABILITIES), TIMESTAMP_MICROSECONDS);
        assertEquals(getTypeStringForClient(precision9, LEGACY_CAPABILITIES), TIMESTAMP);
        assertEquals(getTypeStringForClient(precision12, LEGACY_CAPABILITIES), TIMESTAMP);
    }
}
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
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;

import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.client.ClientCapabilities.PARAMETRIC_DATETIME;
import static java.lang.String.format;

/**
 * Handles type representation compatibility between parametric timestamps
 * and legacy clients, based on Trino PR #4036 and #4475.
 * <p>
 * This class provides downgrade logic to ensure that legacy clients
 * (those without PARAMETRIC_DATETIME capability) continue to work
 * with parametric timestamp types by receiving simplified type names.
 */
public final class TimestampTypeCompatibility
{
    private TimestampTypeCompatibility() {}

    /**
     * Returns the appropriate type string for the given client capabilities.
     * Modern clients with PARAMETRIC_DATETIME capability receive parametric
     * type names like "timestamp(6)", while legacy clients receive simplified
     * names like "timestamp" or "timestamp_microseconds".
     */
    public static String getTypeStringForClient(Type type, Set<ClientCapabilities> capabilities)
    {
        if (!capabilities.contains(PARAMETRIC_DATETIME)) {
            return downgradeTypeForLegacyClient(type);
        }
        return type.getDisplayName();
    }

    /**
     * Downgrades parametric timestamp types to legacy-compatible type names.
     * This method handles both simple and complex types recursively.
     */
    private static String downgradeTypeForLegacyClient(Type type)
    {
        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            return getDowngradedTimestampName(timestampType.getPrecision());
        }

        if (type instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampTzType = (TimestampWithTimeZoneType) type;
            return getDowngradedTimestampWithTimeZoneName(timestampTzType.getPrecision());
        }

        // Handle complex types recursively
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            String elementTypeString = downgradeTypeForLegacyClient(arrayType.getElementType());
            return format("array(%s)", elementTypeString);
        }

        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            String keyTypeString = downgradeTypeForLegacyClient(mapType.getKeyType());
            String valueTypeString = downgradeTypeForLegacyClient(mapType.getValueType());
            return format("map(%s, %s)", keyTypeString, valueTypeString);
        }

        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            return formatRowType(rowType);
        }

        return type.getDisplayName();
    }

    /**
     * Returns the legacy type name for timestamp with the given precision.
     * Follows the same logic as Trino's implementation:
     * - Precision 6 maps to "timestamp_microseconds" for specific compatibility
     * - All other precisions map to "timestamp" (the original legacy type)
     */
    private static String getDowngradedTimestampName(int precision)
    {
        if (precision == 6) {
            return StandardTypes.TIMESTAMP_MICROSECONDS;
        }
        return StandardTypes.TIMESTAMP;
    }

    /**
     * Returns the legacy type name for timestamp with time zone with the given precision.
     * Follows the same logic as Trino's implementation:
     * - Precision 6 maps to "timestamp with time zone microseconds"
     * - All other precisions map to "timestamp with time zone"
     */
    private static String getDowngradedTimestampWithTimeZoneName(int precision)
    {
        if (precision == 6) {
            return StandardTypes.TIMESTAMP_WITH_TIME_ZONE_MICROSECONDS;
        }
        return StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
    }

    /**
     * Formats a ROW type with downgraded field types for legacy clients.
     * This ensures that complex types containing parametric timestamps
     * are properly downgraded recursively.
     */
    private static String formatRowType(RowType rowType)
    {
        String fields = rowType.getFields().stream()
                .map(field -> {
                    String fieldType = downgradeTypeForLegacyClient(field.getType());
                    if (field.getName().isPresent()) {
                        return field.getName().get() + " " + fieldType;
                    }
                    return fieldType;
                })
                .collect(Collectors.joining(", "));
        return format("row(%s)", fields);
    }
}
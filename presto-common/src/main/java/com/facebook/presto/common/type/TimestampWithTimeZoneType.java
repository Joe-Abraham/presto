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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;

import java.util.Objects;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;

public final class TimestampWithTimeZoneType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;
    public static final int DEFAULT_PRECISION = 3;

    private static final TimestampWithTimeZoneType[] INSTANCES = new TimestampWithTimeZoneType[MAX_PRECISION + 1];

    static {
        for (int p = 0; p <= MAX_PRECISION; p++) {
            INSTANCES[p] = new TimestampWithTimeZoneType(p);
        }
    }

    // Preserves "timestamp with time zone" (no parameter) so existing serialized metadata continues to parse.
    public static final TimestampWithTimeZoneType TIMESTAMP_WITH_TIME_ZONE = INSTANCES[DEFAULT_PRECISION];

    private final int precision;

    public static TimestampWithTimeZoneType createTimestampWithTimeZoneType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format(
                    "TIMESTAMP WITH TIME ZONE precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return INSTANCES[precision];
    }

    private TimestampWithTimeZoneType(int precision)
    {
        super(buildTypeSignature(precision));
        this.precision = precision;
    }

    private static TypeSignature buildTypeSignature(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Preserve "timestamp with time zone" (no parameter) so existing serialized metadata continues to parse.
            return parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        }
        // Other precisions use a numeric parameter; the type registry does not yet recognize the
        // "timestamp with time zone(p)" string form, so these instances are created directly rather than parsed.
        return new TypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, TypeSignatureParameter.of((long) precision));
    }

    public int getPrecision()
    {
        return precision;
    }

    /**
     * Timestamp with time zone represents a single point in time.  Multiple timestamps with timezones may
     * each refer to the same point in time.  For example, 9:00am in New York is the same point in time as
     * 2:00pm in London.  While those two timestamps may be encoded differently, they each refer to the same
     * point in time.  Therefore, it's possible encode multiple timestamps which each represent the same
     * point in time, and hence it's not safe to use equality as a proxy for identity.
     */
    @Override
    public boolean equalValuesAreIdentical()
    {
        return false;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return new SqlTimestampWithTimeZone(block.getLong(position));
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = unpackMillisUtc(leftBlock.getLong(leftPosition));
        long rightValue = unpackMillisUtc(rightBlock.getLong(rightPosition));
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return AbstractLongType.hash(unpackMillisUtc(block.getLong(position)));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = unpackMillisUtc(leftBlock.getLong(leftPosition));
        long rightValue = unpackMillisUtc(rightBlock.getLong(rightPosition));
        return Long.compare(leftValue, rightValue);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        // One interned instance per precision level, so reference equality is sufficient.
        return this == other;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), precision);
    }
}

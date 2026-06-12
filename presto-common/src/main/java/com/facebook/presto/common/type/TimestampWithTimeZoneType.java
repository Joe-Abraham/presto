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

import static com.facebook.presto.common.type.Timestamps.MAX_PRECISION;
import static java.lang.String.format;

/**
 * TIMESTAMP WITH TIME ZONE type with precision 0..12.
 * <p>
 * Precision 0-3 (short timestamps with time zone) are stored as a single packed {@code long}:
 * milliseconds UTC in bits 63:12, timezone key in bits 11:0.
 * <p>
 * Precision 4-12 (long timestamps with time zone) are stored as 12 bytes using
 * {@link com.facebook.presto.common.block.Fixed12Block}: the first 8 bytes store epoch
 * milliseconds and the last 4 bytes store {@code (nanosOfMilli << 12) | (timeZoneKey & 0xFFF)}.
 * <p>
 * Backward compatibility:
 * <ul>
 *   <li>{@link #TIMESTAMP_WITH_TIME_ZONE} is an alias for precision 3 (millisecond precision)</li>
 * </ul>
 */
public abstract class TimestampWithTimeZoneType
        extends AbstractType
        implements FixedWidthType
{
    public static final int MAX_SHORT_PRECISION = 3;

    // Pre-defined instances for common precisions
    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_MILLIS = createTimestampWithTimeZoneType(3);

    // Backward-compatible alias (precision 3 = milliseconds, matching the original single type)
    public static final TimestampWithTimeZoneType TIMESTAMP_WITH_TIME_ZONE = TIMESTAMP_TZ_MILLIS;

    private final int precision;

    TimestampWithTimeZoneType(int precision, Class<?> javaType)
    {
        super(javaType);
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("TIMESTAMP WITH TIME ZONE precision must be in range [0, %d]: %s", MAX_PRECISION, precision));
        }
        this.precision = precision;
    }

    /**
     * Creates a TimestampWithTimeZoneType with the given precision (0..12).
     */
    public static TimestampWithTimeZoneType createTimestampWithTimeZoneType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("TIMESTAMP WITH TIME ZONE precision must be in range [0, %d]: %s", MAX_PRECISION, precision));
        }
        if (precision <= MAX_SHORT_PRECISION) {
            return new ShortTimestampWithTimeZoneType(precision);
        }
        return new LongTimestampWithTimeZoneType(precision);
    }

    /**
     * Returns the precision (0..12) of this type.
     */
    public int getPrecision()
    {
        return precision;
    }

    /**
     * Returns true if this is a short timestamp with time zone (precision 0..3), stored as a single long.
     */
    public boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    /**
     * Returns true if this is a long timestamp with time zone (precision 4..12), stored as 12 bytes.
     */
    public boolean isLong()
    {
        return precision > MAX_SHORT_PRECISION;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    /**
     * Timestamp with time zone represents a single point in time. Multiple timestamps with timezones may
     * each refer to the same point in time. For example, 9:00am in New York is the same point in time as
     * 2:00pm in London. While those two timestamps may be encoded differently, they each refer to the same
     * point in time. Therefore, it is possible to encode multiple timestamps which each represent the same
     * point in time, and hence it is not safe to use equality as a proxy for identity.
     */
    @Override
    public boolean equalValuesAreIdentical()
    {
        return false;
    }

    @Override
    public abstract Object getObjectValue(SqlFunctionProperties properties, Block block, int position);

    @Override
    public TypeSignature getTypeSignature()
    {
        if (precision == 3) {
            // Backward compatible: no precision parameter for the default millisecond precision
            return TypeSignature.parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        }
        return new TypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, TypeSignatureParameter.of(precision));
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TimestampWithTimeZoneType)) {
            return false;
        }
        return this.precision == ((TimestampWithTimeZoneType) other).precision;
    }

    @Override
    public int hashCode()
    {
        return precision;
    }
}

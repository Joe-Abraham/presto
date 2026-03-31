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

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

//
// TIMESTAMP(p) stores epoch nanoseconds for all precisions (0-12).
// This provides a uniform storage model; the precision controls how many
// fractional-second digits are significant when displaying or comparing.
//
public final class TimestampType
        extends AbstractLongType
{
    public static final int DEFAULT_PRECISION = 3;
    public static final int MAX_PRECISION = 12;

    // One singleton per precision to avoid redundant instances.
    private static final TimestampType[] INSTANCES;

    static {
        INSTANCES = new TimestampType[MAX_PRECISION + 1];
        for (int p = 0; p <= MAX_PRECISION; p++) {
            INSTANCES[p] = new TimestampType(p);
        }
    }
    // Backward-compatible constants.
    public static final TimestampType TIMESTAMP = createTimestampType(DEFAULT_PRECISION);
    public static final TimestampType TIMESTAMP_MICROSECONDS = createTimestampType(6);

    private final int precision;

    private TimestampType(int precision)
    {
        super(new TypeSignature(StandardTypes.TIMESTAMP, TypeSignatureParameter.of((long) precision)));
        this.precision = precision;
    }

    /**
     * Creates a {@code TimestampType} for the given fractional-seconds precision {@code p} (0-12).
     * All precisions use nanosecond storage.
     */
    public static TimestampType createTimestampType(int precision)
    {
        checkArgument(precision >= 0 && precision <= MAX_PRECISION,
                "Invalid TIMESTAMP precision %s; must be in range [0, %s]", precision, MAX_PRECISION);
        return INSTANCES[precision];
    }

    /**
     * Returns the fractional-seconds precision of this type (0-12).
     */
    public int getPrecision()
    {
        return precision;
    }

    /**
     * Returns {@code true} when this type's precision exceeds the default (milliseconds).
     * Callers should treat stored values as nanoseconds regardless; this flag indicates
     * whether sub-millisecond precision is significant for the declared type.
     */
    public boolean isLongTimestamp()
    {
        return precision > DEFAULT_PRECISION;
    }

    /**
     * Returns {@link TimeUnit#NANOSECONDS} — the universal storage unit for all precisions.
     */
    public TimeUnit getStorageUnit()
    {
        return NANOSECONDS;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        long nanos = block.getLong(position);
        // Scale nanoseconds down to the display unit matching this type's precision.
        TimeUnit displayUnit;
        long displayValue;
        if (precision <= 3) {
            displayUnit = MILLISECONDS;
            displayValue = NANOSECONDS.toMillis(nanos);
        }
        else if (precision <= 6) {
            displayUnit = MICROSECONDS;
            displayValue = NANOSECONDS.toMicros(nanos);
        }
        else {
            displayUnit = NANOSECONDS;
            displayValue = nanos;
        }

        if (properties.isLegacyTimestamp()) {
            return new SqlTimestamp(displayValue, properties.getTimeZoneKey(), displayUnit);
        }
        else {
            return new SqlTimestamp(displayValue, displayUnit);
        }
    }

    /**
     * Gets the timestamp's total epoch seconds.
     */
    public long getEpochSecond(long timestamp)
    {
        return NANOSECONDS.toSeconds(timestamp);
    }

    /**
     * Gets the timestamp's nanosecond portion (sub-second).
     */
    public int getNanos(long timestamp)
    {
        long nanosPerSecond = TimeUnit.SECONDS.toNanos(1);
        return (int) (timestamp % nanosPerSecond);
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof TimestampType && ((TimestampType) other).precision == this.precision;
    }

    @Override
    public int hashCode()
    {
        return Integer.hashCode(precision);
    }
}

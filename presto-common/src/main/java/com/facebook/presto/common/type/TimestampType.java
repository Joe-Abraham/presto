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
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

//
// TIMESTAMP(p) stores epoch milliseconds for p <= 3, and epoch microseconds for p > 3.
// This covers precision 0 through 12.  The two widely-used singletons TIMESTAMP (p=3, ms)
// and TIMESTAMP_MICROSECONDS (p=6, µs) are kept for backward compatibility.
//
public final class TimestampType
        extends AbstractLongType
{
    public static final int DEFAULT_PRECISION = 3;
    public static final int MAX_PRECISION = 12;

    // Precision boundary: p <= SHORT_PRECISION uses ms storage, p > SHORT_PRECISION uses µs.
    private static final int SHORT_PRECISION = DEFAULT_PRECISION;

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
     * Creates a {@code TimestampType} for the given fractional-seconds precision {@code p} (0–12).
     * Precision 0–3 uses millisecond storage; precision 4–12 uses microsecond storage.
     */
    public static TimestampType createTimestampType(int precision)
    {
        checkArgument(precision >= 0 && precision <= MAX_PRECISION,
                "Invalid TIMESTAMP precision %s; must be in range [0, %s]", precision, MAX_PRECISION);
        return INSTANCES[precision];
    }

    /**
     * Returns the fractional-seconds precision of this type (0–12).
     */
    public int getPrecision()
    {
        return precision;
    }

    /**
     * Returns {@code true} if this timestamp uses microsecond storage (i.e. precision &gt; 3).
     */
    public boolean isLongTimestamp()
    {
        return precision > SHORT_PRECISION;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        TimeUnit unit = isLongTimestamp() ? MICROSECONDS : MILLISECONDS;
        if (properties.isLegacyTimestamp()) {
            return new SqlTimestamp(block.getLong(position), properties.getTimeZoneKey(), unit);
        }
        else {
            return new SqlTimestamp(block.getLong(position), unit);
        }
    }

    /**
     * Gets the timestamp's total epoch seconds.
     */
    public long getEpochSecond(long timestamp)
    {
        return isLongTimestamp() ? MICROSECONDS.toSeconds(timestamp) : MILLISECONDS.toSeconds(timestamp);
    }

    /**
     * Gets the timestamp's nanosecond portion (sub-second).
     */
    public int getNanos(long timestamp)
    {
        TimeUnit unit = isLongTimestamp() ? MICROSECONDS : MILLISECONDS;
        long unitsPerSecond = unit.convert(1, TimeUnit.SECONDS);
        return (int) unit.toNanos(timestamp % unitsPerSecond);
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof TimestampType && ((TimestampType) other).precision == this.precision;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(TimestampType.class, precision);
    }
}

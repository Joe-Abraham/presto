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

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

//
// TIMESTAMP is stored as milliseconds from 1970-01-01T00:00:00 UTC.  When performing calculations
// on a timestamp the client's time zone must be taken into account.
// TIMESTAMP_MICROSECONDS is stored as microseconds from 1970-01-01T00:00:00 UTC.  When performing calculations
// on a timestamp the client's time zone must be taken into account.
//
public final class TimestampType
        extends AbstractLongType
{
    public static final int DEFAULT_PRECISION = 3;
    public static final int MICROSECONDS_PRECISION = 6;
    public static final int MAX_PRECISION = 12;

    public static final TimestampType TIMESTAMP = new TimestampType(MILLISECONDS);
    public static final TimestampType TIMESTAMP_MICROSECONDS = new TimestampType(MICROSECONDS);

    private final TimeUnit precision;

    private TimestampType(TimeUnit precision)
    {
        super(parseTypeSignature(getTypeName(precision)));
        this.precision = precision;
    }

    /**
     * Creates a {@link TimestampType} with the given fractional-seconds precision.
     * <p>
     * Precision values 0–3 map to {@code timestamp} (millisecond-backed); precision values 4–12
     * map to {@code timestamp microseconds} (microsecond-backed).  Presto's internal storage tops
     * out at microseconds, so precisions 7–12 are accepted but stored at µs resolution (i.e. the
     * effective precision is 6 for any p > 6).
     * <p>
     * This range matches the SQL standard and is consistent with Trino's TIMESTAMP(p) semantics,
     * minus nanosecond support (p > 6).
     *
     * @param precision fractional-seconds digits, 0 ≤ precision ≤ 12
     * @return the appropriate {@link TimestampType} singleton
     * @throws IllegalArgumentException if precision is outside the range [0, 12]
     */
    public static TimestampType createTimestampType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException("TIMESTAMP precision must be between 0 and " + MAX_PRECISION + ", got: " + precision);
        }
        // p=0..3 → millisecond backing store; p=4..12 → microsecond backing store
        if (precision <= DEFAULT_PRECISION) {
            return TIMESTAMP;
        }
        return TIMESTAMP_MICROSECONDS;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (properties.isLegacyTimestamp()) {
            return new SqlTimestamp(block.getLong(position), properties.getTimeZoneKey(), precision);
        }
        else {
            return new SqlTimestamp(block.getLong(position), precision);
        }
    }

    public TimeUnit getPrecision()
    {
        return this.precision;
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        if (precision == MICROSECONDS) {
            return other == TIMESTAMP_MICROSECONDS;
        }
        if (precision == MILLISECONDS) {
            return other == TIMESTAMP;
        }
        throw new UnsupportedOperationException("Unsupported precision " + precision);
    }

    @Override
    public int hashCode()
    {
        return getTypeSignature().hashCode();
    }

    /**
     * Gets the timestamp's number of total seconds.
     * The epoch second count is a simple incrementing count of seconds where second 0 is 1970-01-01T00:00:00Z.
     *
     * Returns:
     * the total seconds in timestamp
     */
    public long getEpochSecond(long timestamp)
    {
        return this.precision.toSeconds(timestamp);
    }

    /**
     * Gets the timestamp's nanosecond portion.
     *
     * Returns:
     * this timestamp's fractional seconds component
     */
    public int getNanos(long timestamp)
    {
        long unitsPerSecond = precision.convert(1, TimeUnit.SECONDS);
        return (int) precision.toNanos(timestamp % unitsPerSecond);
    }

    private static String getTypeName(TimeUnit precision)
    {
        if (precision == MICROSECONDS) {
            return StandardTypes.TIMESTAMP_MICROSECONDS;
        }
        if (precision == MILLISECONDS) {
            return StandardTypes.TIMESTAMP;
        }
        throw new IllegalArgumentException("Unsupported precision " + precision);
    }
}

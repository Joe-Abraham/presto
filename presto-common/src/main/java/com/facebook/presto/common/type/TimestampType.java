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

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Parameterised TIMESTAMP type supporting precisions 0, 3, 6, and 9.
 *
 * <ul>
 *   <li>Precision 0 – stored as <em>seconds</em> since 1970-01-01T00:00:00 UTC
 *       ({@link #TIMESTAMP_SECONDS}). SQL name: {@code timestamp(0)}.</li>
 *   <li>Precision 3 – stored as <em>milliseconds</em> since epoch
 *       ({@link #TIMESTAMP}). SQL name: {@code timestamp} (default, backward-compatible).</li>
 *   <li>Precision 6 – stored as <em>microseconds</em> since epoch
 *       ({@link #TIMESTAMP_MICROSECONDS}). SQL name: {@code timestamp(6)}.</li>
 *   <li>Precision 9 – stored as <em>nanoseconds</em> since epoch
 *       ({@link #TIMESTAMP_NANOS}). SQL name: {@code timestamp(9)}.</li>
 * </ul>
 *
 * When performing calculations on a timestamp the client's time zone must be taken into account.
 */
public final class TimestampType
        extends AbstractLongType
{
    /** Default precision 3 – milliseconds (backward-compatible). */
    public static final TimestampType TIMESTAMP = new TimestampType(MILLISECONDS);
    /** Precision 6 – microseconds. */
    public static final TimestampType TIMESTAMP_MICROSECONDS = new TimestampType(MICROSECONDS);
    /** Precision 0 – seconds. */
    public static final TimestampType TIMESTAMP_SECONDS = new TimestampType(SECONDS);
    /** Precision 9 – nanoseconds. */
    public static final TimestampType TIMESTAMP_NANOS = new TimestampType(NANOSECONDS);

    private final TimeUnit precision;

    private TimestampType(TimeUnit precision)
    {
        super(parseTypeSignature(getTypeName(precision)));
        this.precision = precision;
    }

    /**
     * Returns the {@link TimestampType} for the given decimal precision value.
     *
     * @param precision 0 (seconds), 3 (milliseconds), 6 (microseconds), or 9 (nanoseconds)
     */
    public static TimestampType createTimestampType(int precision)
    {
        switch (precision) {
            case 0:
                return TIMESTAMP_SECONDS;
            case 3:
                return TIMESTAMP;
            case 6:
                return TIMESTAMP_MICROSECONDS;
            case 9:
                return TIMESTAMP_NANOS;
            default:
                throw new IllegalArgumentException("Invalid TIMESTAMP precision " + precision + ". Supported values: 0, 3, 6, 9");
        }
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

    /**
     * Returns the internal {@link TimeUnit} precision of this timestamp type.
     * The returned value is one of {@link TimeUnit#SECONDS}, {@link TimeUnit#MILLISECONDS},
     * {@link TimeUnit#MICROSECONDS}, or {@link TimeUnit#NANOSECONDS}.
     */
    public TimeUnit getPrecision()
    {
        return this.precision;
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        if (precision == SECONDS) {
            return other == TIMESTAMP_SECONDS;
        }
        if (precision == MILLISECONDS) {
            return other == TIMESTAMP;
        }
        if (precision == MICROSECONDS) {
            return other == TIMESTAMP_MICROSECONDS;
        }
        if (precision == NANOSECONDS) {
            return other == TIMESTAMP_NANOS;
        }
        throw new UnsupportedOperationException("Unsupported precision " + precision);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), precision);
    }

    /**
     * Gets the timestamp's number of total seconds.
     * The epoch second count is a simple incrementing count of seconds where second 0 is 1970-01-01T00:00:00Z.
     *
     * @return the total seconds in timestamp
     */
    public long getEpochSecond(long timestamp)
    {
        return this.precision.toSeconds(timestamp);
    }

    /**
     * Gets the timestamp's nanosecond portion (fractional seconds).
     *
     * @return this timestamp's fractional seconds component in nanoseconds
     */
    public int getNanos(long timestamp)
    {
        long unitsPerSecond = precision.convert(1, TimeUnit.SECONDS);
        return (int) precision.toNanos(timestamp % unitsPerSecond);
    }

    private static String getTypeName(TimeUnit precision)
    {
        if (precision == SECONDS) {
            return "timestamp(0)";
        }
        if (precision == MILLISECONDS) {
            return StandardTypes.TIMESTAMP;
        }
        if (precision == MICROSECONDS) {
            return "timestamp(6)";
        }
        if (precision == NANOSECONDS) {
            return "timestamp(9)";
        }
        throw new IllegalArgumentException("Unsupported precision " + precision);
    }
}

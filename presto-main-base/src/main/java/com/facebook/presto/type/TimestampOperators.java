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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameter;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteralMicros;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.floorDiv;

/**
 * Scalar operators for all {@code TIMESTAMP(p)} types (p = 0–12).
 *
 * <p>Operators use {@code @LiteralParameters("p")} so a single implementation handles every
 * precision.  For simple comparisons the underlying long value is compared directly (works for
 * both millisecond and microsecond storage).  For precision-sensitive operations (SUBTRACT,
 * casts to DATE/TIME, etc.) the {@code @LiteralParameter("p")} argument provides the runtime
 * precision so the code can convert between the two storage units as needed.
 */
public final class TimestampOperators
{
    public static final int MILLISECONDS_PER_HOUR = 60 * 60 * 1000;
    public static final int MILLISECONDS_PER_DAY = MILLISECONDS_PER_HOUR * 24;

    private TimestampOperators() {}

    // -----------------------------------------------------------------------
    // Arithmetic
    // -----------------------------------------------------------------------

    @ScalarOperator(SUBTRACT)
    @LiteralParameters("p")
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long subtract(
            @LiteralParameter("p") Long precision,
            @SqlType("timestamp(p)") long left,
            @SqlType("timestamp(p)") long right)
    {
        long diff = left - right;
        // INTERVAL_DAY_TO_SECOND is stored in milliseconds.
        if (precision > TimestampType.DEFAULT_PRECISION) {
            return TimeUnit.MICROSECONDS.toMillis(diff);
        }
        return diff;
    }

    // -----------------------------------------------------------------------
    // Comparisons  (logic is identical for ms and µs storage)
    // -----------------------------------------------------------------------

    @ScalarOperator(EQUAL)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType("timestamp(p)") long left, @SqlType("timestamp(p)") long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType("timestamp(p)") long left, @SqlType("timestamp(p)") long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType("timestamp(p)") long left, @SqlType("timestamp(p)") long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType("timestamp(p)") long left, @SqlType("timestamp(p)") long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType("timestamp(p)") long left, @SqlType("timestamp(p)") long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType("timestamp(p)") long left, @SqlType("timestamp(p)") long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType("timestamp(p)") long value,
            @SqlType("timestamp(p)") long min,
            @SqlType("timestamp(p)") long max)
    {
        return min <= value && value <= max;
    }

    // -----------------------------------------------------------------------
    // Casts to other types
    // -----------------------------------------------------------------------

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long castToDate(
            @LiteralParameter("p") Long precision,
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(precision, value);
        ISOChronology chronology;
        if (properties.isLegacyTimestamp()) {
            chronology = getChronology(properties.getTimeZoneKey());
            long date = chronology.dayOfYear().roundFloor(valueMs);
            long millis = date + chronology.getZone().getOffset(date);
            return TimeUnit.MILLISECONDS.toDays(millis);
        }
        return floorDiv(valueMs, MILLISECONDS_PER_DAY);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("p")
    @SqlType(StandardTypes.TIME)
    public static long castToTime(
            @LiteralParameter("p") Long precision,
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(precision, value);
        if (properties.isLegacyTimestamp()) {
            return modulo24Hour(getChronology(properties.getTimeZoneKey()), valueMs);
        }
        else {
            return modulo24Hour(valueMs);
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("p")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(
            @LiteralParameter("p") Long precision,
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(precision, value);
        if (properties.isLegacyTimestamp()) {
            int timeMillis = modulo24Hour(getChronology(properties.getTimeZoneKey()), valueMs);
            return packDateTimeWithZone(timeMillis, properties.getTimeZoneKey());
        }
        else {
            ISOChronology localChronology = getChronology(properties.getTimeZoneKey());
            return packDateTimeWithZone(localChronology.getZone().convertLocalToUTC(modulo24Hour(valueMs), false), properties.getTimeZoneKey());
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("p")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castToTimestampWithTimeZone(
            @LiteralParameter("p") Long precision,
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(precision, value);
        if (properties.isLegacyTimestamp()) {
            return packDateTimeWithZone(valueMs, properties.getTimeZoneKey());
        }
        else {
            ISOChronology localChronology = getChronology(properties.getTimeZoneKey());
            return packDateTimeWithZone(localChronology.getZone().convertLocalToUTC(valueMs, false), properties.getTimeZoneKey());
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice castToSlice(
            @LiteralParameter("p") Long precision,
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        if (precision > TimestampType.DEFAULT_PRECISION) {
            // Microsecond precision: format with 6 fractional digits.
            return utf8Slice(printTimestampMicros(value));
        }
        if (properties.isLegacyTimestamp()) {
            return utf8Slice(printTimestampWithoutTimeZone(properties.getTimeZoneKey(), value));
        }
        else {
            return utf8Slice(printTimestampWithoutTimeZone(value));
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long castFromSlice(
            @LiteralParameter("p") Long precision,
            SqlFunctionProperties properties,
            @SqlType("varchar(x)") Slice value)
    {
        if (precision > TimestampType.DEFAULT_PRECISION) {
            try {
                return parseTimestampLiteralMicros(trim(value).toStringUtf8());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
        if (properties.isLegacyTimestamp()) {
            try {
                return parseTimestampWithoutTimeZone(properties.getTimeZoneKey(), trim(value).toStringUtf8());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
        else {
            try {
                return parseTimestampWithoutTimeZone(trim(value).toStringUtf8());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
    }

    /**
     * Cross-precision CAST: converts between any two {@code timestamp(p)} types.
     * If the source and target have the same storage (both ms or both µs) no conversion
     * is needed; otherwise the value is scaled between milliseconds and microseconds.
     */
    @ScalarOperator(CAST)
    @LiteralParameters({"from_p", "to_p"})
    @SqlType("timestamp(to_p)")
    public static long castTimestampToTimestamp(
            @LiteralParameter("from_p") Long fromPrecision,
            @LiteralParameter("to_p") Long toPrecision,
            @SqlType("timestamp(from_p)") long value)
    {
        boolean fromMicros = fromPrecision > TimestampType.DEFAULT_PRECISION;
        boolean toMicros = toPrecision > TimestampType.DEFAULT_PRECISION;
        if (!fromMicros && toMicros) {
            // ms → µs
            return TimeUnit.MILLISECONDS.toMicros(value);
        }
        if (fromMicros && !toMicros) {
            // µs → ms (truncate)
            return TimeUnit.MICROSECONDS.toMillis(value);
        }
        // same storage unit, no conversion
        return value;
    }

    // -----------------------------------------------------------------------
    // Hash / identity
    // -----------------------------------------------------------------------

    @ScalarOperator(HASH_CODE)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("timestamp(p)") long value)
    {
        return AbstractLongType.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType("timestamp(p)") long value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(INDETERMINATE)
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType("timestamp(p)") long value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class TimestampDistinctFromOperator
    {
        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType("timestamp(p)") long left,
                @IsNull boolean leftNull,
                @SqlType("timestamp(p)") long right,
                @IsNull boolean rightNull)
        {
            if (leftNull != rightNull) {
                return true;
            }
            if (leftNull) {
                return false;
            }
            return left != right;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = "timestamp(p)", nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = "timestamp(p)", nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return left.getLong(leftPosition) != right.getLong(rightPosition);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Converts a stored timestamp value to milliseconds, normalising µs→ms when precision > 3.
     */
    private static long toMillis(long precision, long value)
    {
        if (precision > TimestampType.DEFAULT_PRECISION) {
            return TimeUnit.MICROSECONDS.toMillis(value);
        }
        return value;
    }

    /**
     * Formats an epoch-microseconds timestamp value as {@code "yyyy-MM-dd HH:mm:ss.SSSSSS"}.
     */
    private static String printTimestampMicros(long epochMicros)
    {
        long epochSeconds = TimeUnit.MICROSECONDS.toSeconds(epochMicros);
        long microsFraction = epochMicros - TimeUnit.SECONDS.toMicros(epochSeconds);
        if (microsFraction < 0) {
            epochSeconds--;
            microsFraction += 1_000_000L;
        }
        java.time.Instant instant = java.time.Instant.ofEpochSecond(epochSeconds,
                TimeUnit.MICROSECONDS.toNanos(microsFraction));
        java.time.LocalDateTime ldt = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
                ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                ldt.getHour(), ldt.getMinute(), ldt.getSecond(),
                ldt.getNano() / 1000);
    }
}

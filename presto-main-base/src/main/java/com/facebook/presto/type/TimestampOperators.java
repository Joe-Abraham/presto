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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
 * Scalar operators for all {@code TIMESTAMP(p)} types (p = 0-12).
 *
 * <p>Operators use {@code @LiteralParameters("p")} so a single implementation handles every
 * precision. For simple comparisons the underlying long value is compared directly (works for
 * both millisecond and microsecond storage). For precision-sensitive operations (SUBTRACT,
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
            @SqlType("timestamp(p)") long left,
            @SqlType("timestamp(p)") long right)
    {
        // INTERVAL_DAY_TO_SECOND is stored in milliseconds; timestamps are stored in nanoseconds.
        return TimeUnit.NANOSECONDS.toMillis(left - right);
    }

    // -----------------------------------------------------------------------
    // Comparisons (logic is identical for ms and µs storage)
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
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(value);
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
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(value);
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
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(value);
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
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long value)
    {
        long valueMs = toMillis(value);
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
        // Timestamps are stored as nanoseconds; format based on declared precision.
        if (precision > 6) {
            return utf8Slice(printTimestampNanos(value));
        }
        if (precision > TimestampType.DEFAULT_PRECISION) {
            return utf8Slice(printTimestampMicros(TimeUnit.NANOSECONDS.toMicros(value)));
        }
        long millis = toMillis(value);
        if (properties.isLegacyTimestamp()) {
            return utf8Slice(printTimestampWithoutTimeZone(properties.getTimeZoneKey(), millis));
        }
        else {
            return utf8Slice(printTimestampWithoutTimeZone(millis));
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
        // All precisions store nanoseconds; parse returns ms or µs depending on precision.
        if (precision > TimestampType.DEFAULT_PRECISION) {
            try {
                // parseTimestampLiteralMicros returns epoch-microseconds; convert to nanoseconds.
                return TimeUnit.MICROSECONDS.toNanos(parseTimestampLiteralMicros(trim(value).toStringUtf8()));
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
        // This accepts value with or without time zone; parsers return epoch-milliseconds.
        if (properties.isLegacyTimestamp()) {
            try {
                return TimeUnit.MILLISECONDS.toNanos(parseTimestampWithoutTimeZone(properties.getTimeZoneKey(), trim(value).toStringUtf8()));
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
        else {
            try {
                return TimeUnit.MILLISECONDS.toNanos(parseTimestampWithoutTimeZone(trim(value).toStringUtf8()));
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
    }

    /**
     * Cross-precision CAST: all {@code timestamp(p)} types use the same nanosecond storage,
     * so no conversion is needed between precisions.
     */
    @ScalarOperator(CAST)
    @LiteralParameters({"from_p", "to_p"})
    @SqlType("timestamp(to_p)")
    public static long castTimestampToTimestamp(
            @SqlType("timestamp(from_p)") long value)
    {
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
     * Converts a stored nanosecond timestamp value to milliseconds.
     */
    private static long toMillis(long value)
    {
        return TimeUnit.NANOSECONDS.toMillis(value);
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
        Instant instant = Instant.ofEpochSecond(epochSeconds, TimeUnit.MICROSECONDS.toNanos(microsFraction));
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
                ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                ldt.getHour(), ldt.getMinute(), ldt.getSecond(),
                ldt.getNano() / 1000);
    }

    /**
     * Formats an epoch-nanoseconds timestamp value as {@code "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"}.
     */
    private static String printTimestampNanos(long epochNanos)
    {
        long epochSeconds = TimeUnit.NANOSECONDS.toSeconds(epochNanos);
        long nanosFraction = epochNanos - TimeUnit.SECONDS.toNanos(epochSeconds);
        if (nanosFraction < 0) {
            epochSeconds--;
            nanosFraction += 1_000_000_000L;
        }
        Instant instant = Instant.ofEpochSecond(epochSeconds, nanosFraction);
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%09d",
                ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                ldt.getHour(), ldt.getMinute(), ldt.getSecond(),
                ldt.getNano());
    }
}

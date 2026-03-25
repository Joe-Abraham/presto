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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
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
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteralMicros;
import static com.facebook.presto.util.DateTimeUtils.printTimestampMicrosWithoutTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;

/**
 * Scalar operators for the {@code timestamp(6)} (microsecond precision) type.
 *
 * <p>Values are stored as <em>microseconds</em> since 1970-01-01T00:00:00 UTC.
 */
public final class TimestampMicrosecondsOperators
{
    /** {@code timestamp(6)} type-name constant used in {@code @SqlType} annotations. */
    public static final String TIMESTAMP_MICROS = "timestamp(6)";

    public static final long MICROSECONDS_PER_MILLISECOND = 1_000L;
    public static final long MICROSECONDS_PER_DAY = 24L * 60 * 60 * 1_000 * MICROSECONDS_PER_MILLISECOND;

    private TimestampMicrosecondsOperators() {}

    @ScalarOperator(SUBTRACT)
    @SqlType("interval day to second")
    public static long subtract(@SqlType(TIMESTAMP_MICROS) long left, @SqlType(TIMESTAMP_MICROS) long right)
    {
        // Result is an interval in milliseconds (INTERVAL_DAY_TO_SECOND storage unit)
        return (left - right) / MICROSECONDS_PER_MILLISECOND;
    }

    @ScalarOperator(EQUAL)
    @SqlType("boolean")
    @SqlNullable
    public static Boolean equal(@SqlType(TIMESTAMP_MICROS) long left, @SqlType(TIMESTAMP_MICROS) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType("boolean")
    @SqlNullable
    public static Boolean notEqual(@SqlType(TIMESTAMP_MICROS) long left, @SqlType(TIMESTAMP_MICROS) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType("boolean")
    public static boolean lessThan(@SqlType(TIMESTAMP_MICROS) long left, @SqlType(TIMESTAMP_MICROS) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType("boolean")
    public static boolean lessThanOrEqual(@SqlType(TIMESTAMP_MICROS) long left, @SqlType(TIMESTAMP_MICROS) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType("boolean")
    public static boolean greaterThan(@SqlType(TIMESTAMP_MICROS) long left, @SqlType(TIMESTAMP_MICROS) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType("boolean")
    public static boolean greaterThanOrEqual(@SqlType(TIMESTAMP_MICROS) long left, @SqlType(TIMESTAMP_MICROS) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType("boolean")
    public static boolean between(@SqlType(TIMESTAMP_MICROS) long value, @SqlType(TIMESTAMP_MICROS) long min, @SqlType(TIMESTAMP_MICROS) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @SqlType("date")
    public static long castToDate(SqlFunctionProperties properties, @SqlType(TIMESTAMP_MICROS) long value)
    {
        if (properties.isLegacyTimestamp()) {
            ISOChronology chronology = getChronology(properties.getTimeZoneKey());
            long millis = value / MICROSECONDS_PER_MILLISECOND;
            long date = chronology.dayOfYear().roundFloor(millis);
            long utcMillis = date + chronology.getZone().getOffset(date);
            return TimeUnit.MILLISECONDS.toDays(utcMillis);
        }
        return value / MICROSECONDS_PER_DAY;
    }

    @ScalarOperator(CAST)
    @SqlType("time")
    public static long castToTime(SqlFunctionProperties properties, @SqlType(TIMESTAMP_MICROS) long value)
    {
        long millis = value / MICROSECONDS_PER_MILLISECOND;
        if (properties.isLegacyTimestamp()) {
            return modulo24Hour(getChronology(properties.getTimeZoneKey()), millis);
        }
        else {
            return modulo24Hour(millis);
        }
    }

    @ScalarOperator(CAST)
    @SqlType("timestamp with time zone")
    public static long castToTimestampWithTimeZone(SqlFunctionProperties properties, @SqlType(TIMESTAMP_MICROS) long value)
    {
        long millis = value / MICROSECONDS_PER_MILLISECOND;
        if (properties.isLegacyTimestamp()) {
            return packDateTimeWithZone(millis, properties.getTimeZoneKey());
        }
        else {
            ISOChronology localChronology = getChronology(properties.getTimeZoneKey());
            return packDateTimeWithZone(localChronology.getZone().convertLocalToUTC(millis, false), properties.getTimeZoneKey());
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(SqlFunctionProperties properties, @SqlType(TIMESTAMP_MICROS) long value)
    {
        if (properties.isLegacyTimestamp()) {
            return utf8Slice(printTimestampMicrosWithoutTimeZone(properties.getTimeZoneKey(), value));
        }
        else {
            return utf8Slice(printTimestampMicrosWithoutTimeZone(value));
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(TIMESTAMP_MICROS)
    public static long castFromSlice(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice value)
    {
        try {
            return parseTimestampLiteralMicros(trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp(6): " + value.toStringUtf8(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Cross-precision casts: TIMESTAMP (ms) ↔ timestamp(6) (µs)
    // -----------------------------------------------------------------------

    /** Widens a millisecond-precision TIMESTAMP to timestamp(6) (microseconds). */
    @ScalarOperator(CAST)
    @SqlType(TIMESTAMP_MICROS)
    public static long castFromTimestamp(@SqlType("timestamp") long valueMillis)
    {
        return valueMillis * MICROSECONDS_PER_MILLISECOND;
    }

    /** Narrows a timestamp(6) (microseconds) to a millisecond-precision TIMESTAMP. */
    @ScalarOperator(CAST)
    @SqlType("timestamp")
    public static long castToTimestamp(@SqlType(TIMESTAMP_MICROS) long valueMicros)
    {
        return valueMicros / MICROSECONDS_PER_MILLISECOND;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType("bigint")
    public static long hashCode(@SqlType(TIMESTAMP_MICROS) long value)
    {
        return AbstractLongType.hash(value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class TimestampMicrosecondsDistinctFromOperator
    {
        @SqlType("boolean")
        public static boolean isDistinctFrom(
                @SqlType(TIMESTAMP_MICROS) long left,
                @IsNull boolean leftNull,
                @SqlType(TIMESTAMP_MICROS) long right,
                @IsNull boolean rightNull)
        {
            if (leftNull != rightNull) {
                return true;
            }
            if (leftNull) {
                return false;
            }
            return notEqual(left, right);
        }

        @SqlType("boolean")
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = TIMESTAMP_MICROS, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = TIMESTAMP_MICROS, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(TIMESTAMP_MICROSECONDS.getLong(left, leftPosition), TIMESTAMP_MICROSECONDS.getLong(right, rightPosition));
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType("boolean")
    public static boolean indeterminate(@SqlType(TIMESTAMP_MICROS) long value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType("bigint")
    public static long xxHash64(@SqlType(TIMESTAMP_MICROS) long value)
    {
        return XxHash64.hash(value);
    }
}

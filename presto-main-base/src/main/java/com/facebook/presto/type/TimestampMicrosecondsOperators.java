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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

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
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteralMicros;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;

/**
 * Scalar operators for the {@code TIMESTAMP MICROSECONDS} (a.k.a. {@code TIMESTAMP(p)} with
 * p&nbsp;∈&nbsp;[4,&nbsp;12]) type.  Values are stored as epoch microseconds (long).
 */
public final class TimestampMicrosecondsOperators
{
    private TimestampMicrosecondsOperators() {}

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long subtract(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right)
    {
        // Return the difference in milliseconds (INTERVAL_DAY_TO_SECOND is stored in ms).
        return TimeUnit.MICROSECONDS.toMillis(left - right);
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long value,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long min,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(
            SqlFunctionProperties properties,
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long value)
    {
        // Format as seconds + microseconds fraction (6 digits).
        long epochSeconds = TimeUnit.MICROSECONDS.toSeconds(value);
        long micros = value - TimeUnit.SECONDS.toMicros(epochSeconds);
        if (micros < 0) {
            epochSeconds--;
            micros += 1_000_000L;
        }
        java.time.Instant instant = java.time.Instant.ofEpochSecond(epochSeconds, TimeUnit.MICROSECONDS.toNanos(micros));
        java.time.LocalDateTime ldt = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
        String formatted = String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
                ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                ldt.getHour(), ldt.getMinute(), ldt.getSecond(),
                TimeUnit.NANOSECONDS.toMicros(ldt.getNano()));
        return utf8Slice(formatted);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS)
    public static long castFromSlice(
            SqlFunctionProperties properties,
            @SqlType("varchar(x)") Slice value)
    {
        try {
            return parseTimestampLiteralMicros(trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp microseconds: " + value.toStringUtf8(), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castToTimestamp(@SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long value)
    {
        // Truncate from microseconds to milliseconds.
        return TimeUnit.MICROSECONDS.toMillis(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS)
    public static long castFromTimestamp(@SqlType(StandardTypes.TIMESTAMP) long value)
    {
        // Extend from milliseconds to microseconds.
        return TimeUnit.MILLISECONDS.toMicros(value);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long value)
    {
        return AbstractLongType.hash(value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class TimestampMicrosecondsDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long right,
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

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.TIMESTAMP_MICROSECONDS, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.TIMESTAMP_MICROSECONDS, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return TIMESTAMP_MICROSECONDS.getLong(left, leftPosition) != TIMESTAMP_MICROSECONDS.getLong(right, rightPosition);
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(
            @SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long value,
            @IsNull boolean isNull)
    {
        return isNull;
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.TIMESTAMP_MICROSECONDS) long value)
    {
        return XxHash64.hash(value);
    }
}

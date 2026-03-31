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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.updateMillisUtc;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class DateTimeOperators
{
    private static final DateTimeField MILLIS_OF_DAY = ISOChronology.getInstanceUTC().millisOfDay();
    private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

    private DateTimeOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        if (MILLIS_OF_DAY.get(right) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot add hour, minutes or seconds to a date");
        }
        return left + MILLISECONDS.toDays(right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalDayToSecondPlusDate(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.DATE) long right)
    {
        if (MILLIS_OF_DAY.get(left) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot add hour, minutes or seconds to a date");
        }
        return MILLISECONDS.toDays(left) + right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long timePlusIntervalDayToSecond(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return modulo24Hour(getChronology(properties.getTimeZoneKey()), left + right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long intervalDayToSecondPlusTime(SqlFunctionProperties properties, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return modulo24Hour(getChronology(properties.getTimeZoneKey()), left + right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZonePlusIntervalDayToSecond(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc(modulo24Hour(unpackChronology(left), unpackMillisUtc(left) + right), left);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long intervalDayToSecondPlusTimeWithTimeZone(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return updateMillisUtc(modulo24Hour(unpackChronology(right), left + unpackMillisUtc(right)), right);
    }

    @ScalarOperator(ADD)
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long timestampPlusIntervalDayToSecond(
            @SqlType("timestamp(p)") long left,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        // INTERVAL_DAY_TO_SECOND is in milliseconds; timestamps are stored in nanoseconds.
        return left + MILLISECONDS.toNanos(right);
    }

    @ScalarOperator(ADD)
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long intervalDayToSecondPlusTimestamp(
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left,
            @SqlType("timestamp(p)") long right)
    {
        return MILLISECONDS.toNanos(left) + right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZonePlusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc(unpackMillisUtc(left) + right, left);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long intervalDayToSecondPlusTimestampWithTimeZone(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return updateMillisUtc(left + unpackMillisUtc(right), right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long millis = MONTH_OF_YEAR_UTC.add(DAYS.toMillis(left), right);
        return MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalYearToMonthPlusDate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DATE) long right)
    {
        long millis = MONTH_OF_YEAR_UTC.add(DAYS.toMillis(right), left);
        return MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long timePlusIntervalYearToMonth(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long intervalYearToMonthPlusTime(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZonePlusIntervalYearToMonth(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long intervalYearToMonthPlusTimeWithTimeZone(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return right;
    }

    @ScalarOperator(ADD)
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long timestampPlusIntervalYearToMonth(SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long left,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        // Timestamps are stored in nanoseconds; month arithmetic works in milliseconds.
        long leftMs = NANOSECONDS.toMillis(left);
        long resultMs = properties.isLegacyTimestamp()
                ? getChronology(properties.getTimeZoneKey()).monthOfYear().add(leftMs, right)
                : MONTH_OF_YEAR_UTC.add(leftMs, right);
        return MILLISECONDS.toNanos(resultMs);
    }

    @ScalarOperator(ADD)
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long intervalYearToMonthPlusTimestamp(
            SqlFunctionProperties properties,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left,
            @SqlType("timestamp(p)") long right)
    {
        long rightMs = NANOSECONDS.toMillis(right);
        long resultMs = properties.isLegacyTimestamp()
                ? getChronology(properties.getTimeZoneKey()).monthOfYear().add(rightMs, left)
                : MONTH_OF_YEAR_UTC.add(rightMs, left);
        return MILLISECONDS.toNanos(resultMs);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZonePlusIntervalYearToMonth(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return updateMillisUtc(unpackChronology(left).monthOfYear().add(unpackMillisUtc(left), right), left);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long intervalYearToMonthPlusTimestampWithTimeZone(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return updateMillisUtc(unpackChronology(right).monthOfYear().add(unpackMillisUtc(right), left), right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        if (MILLIS_OF_DAY.get(right) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot subtract hour, minutes or seconds from a date");
        }
        return left - MILLISECONDS.toDays(right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME)
    public static long timeMinusIntervalDayToSecond(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return modulo24Hour(getChronology(properties.getTimeZoneKey()), left - right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZoneMinusIntervalDayToSecond(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc(modulo24Hour(unpackChronology(left), unpackMillisUtc(left) - right), left);
    }

    @ScalarOperator(SUBTRACT)
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long timestampMinusIntervalDayToSecond(
            @SqlType("timestamp(p)") long left,
            @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return left - MILLISECONDS.toNanos(right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZoneMinusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc(unpackMillisUtc(left) - right, left);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalYearToMonth(SqlFunctionProperties properties, @SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long millis = MONTH_OF_YEAR_UTC.add(DAYS.toMillis(left), -right);
        return MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME)
    public static long timeMinusIntervalYearToMonth(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZoneMinusIntervalYearToMonth(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(SUBTRACT)
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long timestampMinusIntervalYearToMonth(
            SqlFunctionProperties properties,
            @SqlType("timestamp(p)") long left,
            @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long leftMs = NANOSECONDS.toMillis(left);
        long resultMs = properties.isLegacyTimestamp()
                ? getChronology(properties.getTimeZoneKey()).monthOfYear().add(leftMs, -right)
                : MONTH_OF_YEAR_UTC.add(leftMs, -right);
        return MILLISECONDS.toNanos(resultMs);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZoneMinusIntervalYearToMonth(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long dateTimeWithTimeZone = unpackChronology(left).monthOfYear().add(unpackMillisUtc(left), -right);
        return updateMillisUtc(dateTimeWithTimeZone, left);
    }

    public static int modulo24Hour(ISOChronology chronology, long millis)
    {
        return chronology.millisOfDay().get(millis) - chronology.getZone().getOffset(millis);
    }

    public static long modulo24Hour(long millis)
    {
        return MILLIS_OF_DAY.get(millis);
    }
}

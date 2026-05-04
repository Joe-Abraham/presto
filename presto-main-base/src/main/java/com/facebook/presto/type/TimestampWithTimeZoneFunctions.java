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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.type.LongTimestampWithTimeZone;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimeZoneNotSupportedException;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.util.DateTimeZoneIndex.extractZoneOffsetMinutes;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Scalar functions for parametric TIMESTAMP WITH TIME ZONE (all precisions 0–12):
 * <ul>
 *   <li>{@code timezone_hour(timestamp with time zone(p))} → {@code bigint}</li>
 *   <li>{@code timezone_minute(timestamp with time zone(p))} → {@code bigint}</li>
 *   <li>{@code at_timezone(timestamp with time zone(p), varchar)} → {@code timestamp with time zone(p)}</li>
 *   <li>{@code at_timezone(timestamp with time zone(p), interval day to second)} → {@code timestamp with time zone(p)}</li>
 * </ul>
 *
 * <p>Short TSTZ (precision 0–3) are stored as a packed {@code long} (millis UTC + zone key).
 * Long TSTZ (precision 4–12) are stored as {@link LongTimestampWithTimeZone}.
 */
public final class TimestampWithTimeZoneFunctions
{
    private static final TypeSignature TIMESTAMP_TZ_SIGNATURE =
            parseTypeSignature("timestamp with time zone(p)", ImmutableSet.of("p"));

    private static final TypeSignature BIGINT_SIGNATURE = parseTypeSignature(BIGINT);
    private static final TypeSignature INTERVAL_SIGNATURE = parseTypeSignature(INTERVAL_DAY_TO_SECOND);

    // -----------------------------------------------------------------------
    // timezone_hour
    // -----------------------------------------------------------------------

    public static final SqlScalarFunction TIMEZONE_HOUR_FUNCTION =
            SqlScalarFunction.builder(TimestampWithTimeZoneFunctions.class)
                    .signature(SignatureBuilder.builder()
                            .name("timezone_hour")
                            .kind(FunctionKind.SCALAR)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE)
                            .returnType(BIGINT_SIGNATURE)
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("timezoneHourShort", "timezoneHourLong")))
                    .build();

    // -----------------------------------------------------------------------
    // timezone_minute
    // -----------------------------------------------------------------------

    public static final SqlScalarFunction TIMEZONE_MINUTE_FUNCTION =
            SqlScalarFunction.builder(TimestampWithTimeZoneFunctions.class)
                    .signature(SignatureBuilder.builder()
                            .name("timezone_minute")
                            .kind(FunctionKind.SCALAR)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE)
                            .returnType(BIGINT_SIGNATURE)
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("timezoneMinuteShort", "timezoneMinuteLong")))
                    .build();

    // -----------------------------------------------------------------------
    // at_timezone (varchar)
    // -----------------------------------------------------------------------

    public static final SqlScalarFunction AT_TIMEZONE_WITH_ZONE_ID_FUNCTION =
            SqlScalarFunction.builder(TimestampWithTimeZoneFunctions.class)
                    .signature(SignatureBuilder.builder()
                            .name("at_timezone")
                            .kind(FunctionKind.SCALAR)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
                            .returnType(TIMESTAMP_TZ_SIGNATURE)
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("atTimezoneZoneIdShort", "atTimezoneZoneIdLong")))
                    .build();

    // -----------------------------------------------------------------------
    // at_timezone (interval day to second)
    // -----------------------------------------------------------------------

    public static final SqlScalarFunction AT_TIMEZONE_WITH_OFFSET_FUNCTION =
            SqlScalarFunction.builder(TimestampWithTimeZoneFunctions.class)
                    .signature(SignatureBuilder.builder()
                            .name("at_timezone")
                            .kind(FunctionKind.SCALAR)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, INTERVAL_SIGNATURE)
                            .returnType(TIMESTAMP_TZ_SIGNATURE)
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("atTimezoneOffsetShort", "atTimezoneOffsetLong")))
                    .build();

    private TimestampWithTimeZoneFunctions() {}

    // -----------------------------------------------------------------------
    // timezone_hour implementations
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static long timezoneHourShort(long packedTstz)
    {
        return extractZoneOffsetMinutes(packedTstz) / 60;
    }

    @UsedByGeneratedCode
    public static long timezoneHourLong(LongTimestampWithTimeZone value)
    {
        return extractZoneOffsetMinutes(value.getTimeZoneKey(), value.getEpochMillis()) / 60;
    }

    // -----------------------------------------------------------------------
    // timezone_minute implementations
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static long timezoneMinuteShort(long packedTstz)
    {
        return extractZoneOffsetMinutes(packedTstz) % 60;
    }

    @UsedByGeneratedCode
    public static long timezoneMinuteLong(LongTimestampWithTimeZone value)
    {
        return extractZoneOffsetMinutes(value.getTimeZoneKey(), value.getEpochMillis()) % 60;
    }

    // -----------------------------------------------------------------------
    // at_timezone (varchar) implementations
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static long atTimezoneZoneIdShort(long packedTstz, Slice zoneId)
    {
        try {
            return packDateTimeWithZone(unpackMillisUtc(packedTstz), zoneId.toStringUtf8());
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @UsedByGeneratedCode
    public static LongTimestampWithTimeZone atTimezoneZoneIdLong(LongTimestampWithTimeZone value, Slice zoneId)
    {
        try {
            TimeZoneKey newZoneKey = getTimeZoneKey(zoneId.toStringUtf8());
            return new LongTimestampWithTimeZone(value.getEpochMillis(), value.getNanosOfMilli(), newZoneKey.getKey());
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    // -----------------------------------------------------------------------
    // at_timezone (interval) implementations
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static long atTimezoneOffsetShort(long packedTstz, long zoneOffset)
    {
        checkArgument((zoneOffset % 60_000L) == 0L, "Invalid time zone offset interval: interval contains seconds");
        long zoneOffsetMinutes = zoneOffset / 60_000L;
        try {
            return packDateTimeWithZone(unpackMillisUtc(packedTstz), getTimeZoneKeyForOffset(zoneOffsetMinutes));
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @UsedByGeneratedCode
    public static LongTimestampWithTimeZone atTimezoneOffsetLong(LongTimestampWithTimeZone value, long zoneOffset)
    {
        checkArgument((zoneOffset % 60_000L) == 0L, "Invalid time zone offset interval: interval contains seconds");
        long zoneOffsetMinutes = zoneOffset / 60_000L;
        try {
            TimeZoneKey newZoneKey = getTimeZoneKeyForOffset(zoneOffsetMinutes);
            return new LongTimestampWithTimeZone(value.getEpochMillis(), value.getNanosOfMilli(), newZoneKey.getKey());
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }
}

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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.type.TimeZoneNotSupportedException;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Signature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

// Precision-preserving date_trunc and at_timezone for TIMESTAMP(p) and TIMESTAMP(p) WITH TIME ZONE.
// Uses withVariadicBound("T", ...) so the return type equals the input precision, rather than
// downgrading to the fixed TIMESTAMP(3) returned by the annotation-based overloads.
// Scoped to short precisions (p ≤ 6); p > 6 will fail at function binding time.
public final class TimestampPrecisionFunctions
{
    private static final Signature DATE_TRUNC_TIMESTAMP_SIGNATURE = SignatureBuilder.builder()
            .kind(SCALAR)
            .name("date_trunc")
            .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
            .argumentTypes(parseTypeSignature("varchar"), parseTypeSignature("T"))
            .returnType(parseTypeSignature("T"))
            .build();

    public static final SqlScalarFunction DATE_TRUNC_TIMESTAMP = SqlScalarFunction.builder(TimestampPrecisionFunctions.class)
            .signature(DATE_TRUNC_TIMESTAMP_SIGNATURE)
            .description("truncate to the specified precision in UTC")
            .deterministic(true)
            .calledOnNullInput(false)
            .choice(choice -> choice
                    .implementation(methodsGroup -> methodsGroup
                            .methods("truncateTimestamp")
                            .withExtraParameters(context -> {
                                TimestampType type = (TimestampType) context.getType("T");
                                return ImmutableList.of((long) type.getPrecision());
                            })))
            .build();

    private static final Signature AT_TIMEZONE_TIMESTAMP_TZ_SIGNATURE = SignatureBuilder.builder()
            .kind(SCALAR)
            .name("at_timezone")
            .typeVariableConstraints(withVariadicBound("T", TIMESTAMP_WITH_TIME_ZONE))
            .argumentTypes(parseTypeSignature("T"), parseTypeSignature("varchar"))
            .returnType(parseTypeSignature("T"))
            .build();

    public static final SqlScalarFunction AT_TIMEZONE_TIMESTAMP_TZ = SqlScalarFunction.builder(TimestampPrecisionFunctions.class)
            .signature(AT_TIMEZONE_TIMESTAMP_TZ_SIGNATURE)
            .description("convert timestamp to the given time zone, preserving declared precision")
            .deterministic(true)
            .calledOnNullInput(false)
            .choice(choice -> choice
                    .implementation(methodsGroup -> methodsGroup
                            .methods("atTimezone")))
            .build();

    private TimestampPrecisionFunctions() {}

    @UsedByGeneratedCode
    public static long truncateTimestamp(Slice unit, long timestamp, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        long epochMillis = type.toEpochMillis(timestamp);
        long truncatedMillis = DateTimeFunctions.getTimestampField(ISOChronology.getInstanceUTC(), unit).roundFloor(epochMillis);
        long epochSecond = floorDiv(truncatedMillis, 1000L);
        int nanosFromMillis = (int) (floorMod(truncatedMillis, 1000L) * 1_000_000L);
        return type.fromEpochComponents(epochSecond, nanosFromMillis);
    }

    @UsedByGeneratedCode
    public static long atTimezone(long timestampWithTimeZone, Slice zoneId)
    {
        try {
            return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), zoneId.toStringUtf8());
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

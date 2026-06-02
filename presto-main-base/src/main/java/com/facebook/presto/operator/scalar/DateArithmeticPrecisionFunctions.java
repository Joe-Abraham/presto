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
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

// Precision-preserving date_add, date_diff, and INTERVAL arithmetic for TIMESTAMP(p), p ≤ 6.
// Uses withVariadicBound("T", "timestamp") so the return type equals the input precision.
// Sub-millisecond components are preserved: all supported units are ≥ 1ms, so sub-ms is
// unchanged by the operation itself and is carried through reconstructTimestamp().
public final class DateArithmeticPrecisionFunctions
{
    private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

    public static final SqlScalarFunction DATE_ADD_TIMESTAMP = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .name("date_add")
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("varchar"), parseTypeSignature("bigint"), parseTypeSignature("T"))
                    .returnType(parseTypeSignature("T"))
                    .build())
            .description("add the specified amount of time to the timestamp, preserving declared precision")
            .deterministic(true)
            .calledOnNullInput(false)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("addField")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction DATE_DIFF_TIMESTAMP = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .name("date_diff")
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("varchar"), parseTypeSignature("T"), parseTypeSignature("T"))
                    .returnType(parseTypeSignature("bigint"))
                    .build())
            .description("difference of the given timestamps in the given unit, preserving declared precision")
            .deterministic(true)
            .calledOnNullInput(false)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("diffField")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_ADD_INTERVAL_DAY = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class, ADD)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(ADD)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"), parseTypeSignature(INTERVAL_DAY_TO_SECOND))
                    .returnType(parseTypeSignature("T"))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("addIntervalDayToSecond")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction INTERVAL_DAY_ADD_TIMESTAMP = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class, ADD)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(ADD)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature(INTERVAL_DAY_TO_SECOND), parseTypeSignature("T"))
                    .returnType(parseTypeSignature("T"))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("intervalDayAddTimestamp")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_SUBTRACT_INTERVAL_DAY = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class, SUBTRACT)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(SUBTRACT)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"), parseTypeSignature(INTERVAL_DAY_TO_SECOND))
                    .returnType(parseTypeSignature("T"))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("subtractIntervalDayToSecond")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_ADD_INTERVAL_YEAR = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class, ADD)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(ADD)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"), parseTypeSignature(INTERVAL_YEAR_TO_MONTH))
                    .returnType(parseTypeSignature("T"))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("addIntervalYearToMonth")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction INTERVAL_YEAR_ADD_TIMESTAMP = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class, ADD)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(ADD)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature(INTERVAL_YEAR_TO_MONTH), parseTypeSignature("T"))
                    .returnType(parseTypeSignature("T"))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("intervalYearAddTimestamp")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_SUBTRACT_INTERVAL_YEAR = SqlScalarFunction.builder(DateArithmeticPrecisionFunctions.class, SUBTRACT)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(SUBTRACT)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"), parseTypeSignature(INTERVAL_YEAR_TO_MONTH))
                    .returnType(parseTypeSignature("T"))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("subtractIntervalYearToMonth")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    private DateArithmeticPrecisionFunctions() {}

    @UsedByGeneratedCode
    public static long addField(Slice unit, long value, long timestamp, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        long epochMillis = type.toEpochMillis(timestamp);
        long newEpochMillis = DateTimeFunctions.getTimestampField(ISOChronology.getInstanceUTC(), unit).add(epochMillis, value);
        return reconstructTimestamp(type, timestamp, epochMillis, newEpochMillis);
    }

    @UsedByGeneratedCode
    public static long diffField(Slice unit, long left, long right, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        return DateTimeFunctions.getTimestampField(ISOChronology.getInstanceUTC(), unit)
                .getDifferenceAsLong(type.toEpochMillis(right), type.toEpochMillis(left));
    }

    @UsedByGeneratedCode
    public static long addIntervalDayToSecond(long timestamp, long intervalMillis, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        long epochMillis = type.toEpochMillis(timestamp);
        return reconstructTimestamp(type, timestamp, epochMillis, epochMillis + intervalMillis);
    }

    @UsedByGeneratedCode
    public static long intervalDayAddTimestamp(long intervalMillis, long timestamp, long precision)
    {
        return addIntervalDayToSecond(timestamp, intervalMillis, precision);
    }

    @UsedByGeneratedCode
    public static long subtractIntervalDayToSecond(long timestamp, long intervalMillis, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        long epochMillis = type.toEpochMillis(timestamp);
        return reconstructTimestamp(type, timestamp, epochMillis, epochMillis - intervalMillis);
    }

    @UsedByGeneratedCode
    public static long addIntervalYearToMonth(long timestamp, long months, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        long epochMillis = type.toEpochMillis(timestamp);
        long newEpochMillis = MONTH_OF_YEAR_UTC.add(epochMillis, months);
        return reconstructTimestamp(type, timestamp, epochMillis, newEpochMillis);
    }

    @UsedByGeneratedCode
    public static long intervalYearAddTimestamp(long months, long timestamp, long precision)
    {
        return addIntervalYearToMonth(timestamp, months, precision);
    }

    @UsedByGeneratedCode
    public static long subtractIntervalYearToMonth(long timestamp, long months, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        long epochMillis = type.toEpochMillis(timestamp);
        long newEpochMillis = MONTH_OF_YEAR_UTC.add(epochMillis, -months);
        return reconstructTimestamp(type, timestamp, epochMillis, newEpochMillis);
    }

    // Reconstructs a TIMESTAMP(p) value from a new epoch-millis, carrying forward the
    // sub-millisecond component of the original timestamp unchanged.
    private static long reconstructTimestamp(TimestampType type, long originalTimestamp, long origEpochMillis, long newEpochMillis)
    {
        long origSec = floorDiv(origEpochMillis, 1000L);
        int origNanos = (int) (floorMod(origEpochMillis, 1000L) * 1_000_000L);
        long subMillisNative = originalTimestamp - type.fromEpochComponents(origSec, origNanos);
        long newSec = floorDiv(newEpochMillis, 1000L);
        int newNanos = (int) (floorMod(newEpochMillis, 1000L) * 1_000_000L);
        return type.fromEpochComponents(newSec, newNanos) + subMillisNative;
    }
}

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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.LongTimestampWithTimeZone;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.XxHash64;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_NULL_FLAG;
import static java.util.Arrays.asList;

/**
 * Comparison, hash, and equality operators for parametric TIMESTAMP WITH TIME ZONE (all precisions 0–12).
 *
 * <p>Short TSTZ (precision 0–3) are stored as {@code long} (packed millis+tzkey); long TSTZ
 * (precision 4–12) are stored as {@link LongTimestampWithTimeZone}.
 *
 * <p>TIMESTAMP WITH TIME ZONE equality/ordering is always UTC-based: the timezone affects
 * display only, not the comparison value.
 */
public final class TimestampWithTimeZoneParametricOperators
{
    private static final TypeSignature TIMESTAMP_TZ_SIGNATURE =
            parseTypeSignature("timestamp with time zone(p)", ImmutableSet.of("p"));

    public static final SqlScalarFunction TIMESTAMP_TZ_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(EQUAL)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .nullableResult(true)
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("equalShort", "equalLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_NOT_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, NOT_EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(NOT_EQUAL)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .nullableResult(true)
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("notEqualShort", "notEqualLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_LESS_THAN_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, LESS_THAN)
                    .signature(SignatureBuilder.builder()
                            .operatorType(LESS_THAN)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("lessThanShort", "lessThanLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_LESS_THAN_OR_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, LESS_THAN_OR_EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(LESS_THAN_OR_EQUAL)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("lessThanOrEqualShort", "lessThanOrEqualLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_GREATER_THAN_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, GREATER_THAN)
                    .signature(SignatureBuilder.builder()
                            .operatorType(GREATER_THAN)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("greaterThanShort", "greaterThanLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_GREATER_THAN_OR_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, GREATER_THAN_OR_EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(GREATER_THAN_OR_EQUAL)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("greaterThanOrEqualShort", "greaterThanOrEqualLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_BETWEEN_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, BETWEEN)
                    .signature(SignatureBuilder.builder()
                            .operatorType(BETWEEN)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("betweenShort", "betweenLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_HASH_CODE_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, HASH_CODE)
                    .signature(SignatureBuilder.builder()
                            .operatorType(HASH_CODE)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BIGINT))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("hashCodeShort", "hashCodeLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_XX_HASH_64_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, XX_HASH_64)
                    .signature(SignatureBuilder.builder()
                            .operatorType(XX_HASH_64)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BIGINT))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("xxHash64Short", "xxHash64Long")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_IS_DISTINCT_FROM_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, IS_DISTINCT_FROM)
                    .signature(SignatureBuilder.builder()
                            .operatorType(IS_DISTINCT_FROM)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE, TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .argumentProperties(
                                    valueTypeArgumentProperty(USE_NULL_FLAG),
                                    valueTypeArgumentProperty(USE_NULL_FLAG))
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("distinctShort", "distinctLong")))
                    .choice(choice -> choice
                            .argumentProperties(
                                    valueTypeArgumentProperty(BLOCK_AND_POSITION),
                                    valueTypeArgumentProperty(BLOCK_AND_POSITION))
                            .implementation(methodsGroup -> methodsGroup
                                    .methodWithExplicitJavaTypes("distinctBlockPositionLong",
                                            asList(Optional.of(LongTimestampWithTimeZone.class), Optional.of(LongTimestampWithTimeZone.class)))
                                    .methodWithExplicitJavaTypes("distinctBlockPositionShort",
                                            asList(Optional.of(long.class), Optional.of(long.class)))))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_TZ_INDETERMINATE_OPERATOR =
            SqlScalarFunction.builder(TimestampWithTimeZoneParametricOperators.class, INDETERMINATE)
                    .signature(SignatureBuilder.builder()
                            .operatorType(INDETERMINATE)
                            .argumentTypes(TIMESTAMP_TZ_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .argumentProperties(valueTypeArgumentProperty(USE_NULL_FLAG))
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("indeterminateShort", "indeterminateLong")))
                    .build();

    private TimestampWithTimeZoneParametricOperators() {}

    // -----------------------------------------------------------------------
    // Short TSTZ methods (precision 0–3, Java type = long, packed millis+tzkey)
    // Equality/ordering is UTC-based: compare unpacked milliseconds only.
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static Boolean equalShort(long left, long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @UsedByGeneratedCode
    public static Boolean notEqualShort(long left, long right)
    {
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @UsedByGeneratedCode
    public static boolean lessThanShort(long left, long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @UsedByGeneratedCode
    public static boolean lessThanOrEqualShort(long left, long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }

    @UsedByGeneratedCode
    public static boolean greaterThanShort(long left, long right)
    {
        return unpackMillisUtc(left) > unpackMillisUtc(right);
    }

    @UsedByGeneratedCode
    public static boolean greaterThanOrEqualShort(long left, long right)
    {
        return unpackMillisUtc(left) >= unpackMillisUtc(right);
    }

    @UsedByGeneratedCode
    public static boolean betweenShort(long value, long min, long max)
    {
        long millis = unpackMillisUtc(value);
        return unpackMillisUtc(min) <= millis && millis <= unpackMillisUtc(max);
    }

    @UsedByGeneratedCode
    public static long hashCodeShort(long value)
    {
        return AbstractLongType.hash(unpackMillisUtc(value));
    }

    @UsedByGeneratedCode
    public static long xxHash64Short(long value)
    {
        return XxHash64.hash(unpackMillisUtc(value));
    }

    @UsedByGeneratedCode
    public static boolean distinctShort(long left, boolean leftNull, long right, boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @UsedByGeneratedCode
    public static boolean distinctBlockPositionShort(Block left, int leftPosition, Block right, int rightPosition)
    {
        if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
            return true;
        }
        if (left.isNull(leftPosition)) {
            return false;
        }
        return unpackMillisUtc(left.getLong(leftPosition)) != unpackMillisUtc(right.getLong(rightPosition));
    }

    @UsedByGeneratedCode
    public static boolean indeterminateShort(long value, boolean isNull)
    {
        return isNull;
    }

    // -----------------------------------------------------------------------
    // Long TSTZ methods (precision 4–12, Java type = LongTimestampWithTimeZone)
    // Equality/ordering compares epochMillis first (UTC), then nanosOfMilli.
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static Boolean equalLong(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return left.getEpochMillis() == right.getEpochMillis()
                && left.getNanosOfMilli() == right.getNanosOfMilli();
    }

    @UsedByGeneratedCode
    public static Boolean notEqualLong(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return left.getEpochMillis() != right.getEpochMillis()
                || left.getNanosOfMilli() != right.getNanosOfMilli();
    }

    @UsedByGeneratedCode
    public static boolean lessThanLong(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return left.compareTo(right) < 0;
    }

    @UsedByGeneratedCode
    public static boolean lessThanOrEqualLong(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return left.compareTo(right) <= 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanLong(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return left.compareTo(right) > 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanOrEqualLong(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return left.compareTo(right) >= 0;
    }

    @UsedByGeneratedCode
    public static boolean betweenLong(LongTimestampWithTimeZone value, LongTimestampWithTimeZone min, LongTimestampWithTimeZone max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @UsedByGeneratedCode
    public static long hashCodeLong(LongTimestampWithTimeZone value)
    {
        return AbstractLongType.hash(value.getEpochMillis()) * 31 + value.getNanosOfMilli();
    }

    @UsedByGeneratedCode
    public static long xxHash64Long(LongTimestampWithTimeZone value)
    {
        return XxHash64.hash(value.getEpochMillis()) * 31 + value.getNanosOfMilli();
    }

    @UsedByGeneratedCode
    public static boolean distinctLong(LongTimestampWithTimeZone left, boolean leftNull, LongTimestampWithTimeZone right, boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return left.getEpochMillis() != right.getEpochMillis()
                || left.getNanosOfMilli() != right.getNanosOfMilli();
    }

    @UsedByGeneratedCode
    public static boolean distinctBlockPositionLong(Block left, int leftPosition, Block right, int rightPosition)
    {
        if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
            return true;
        }
        if (left.isNull(leftPosition)) {
            return false;
        }
        long leftEpochMillis = left.getLong(leftPosition, 0);
        int leftFraction = left.getInt(leftPosition);
        long rightEpochMillis = right.getLong(rightPosition, 0);
        int rightFraction = right.getInt(rightPosition);
        // Compare by UTC instant (epochMillis + nanosOfMilli), ignore timezone
        if (leftEpochMillis != rightEpochMillis) {
            return true;
        }
        int leftNanos = LongTimestampWithTimeZone.unpackNanosOfMilli(leftFraction);
        int rightNanos = LongTimestampWithTimeZone.unpackNanosOfMilli(rightFraction);
        return leftNanos != rightNanos;
    }

    @UsedByGeneratedCode
    public static boolean indeterminateLong(LongTimestampWithTimeZone value, boolean isNull)
    {
        return isNull;
    }
}

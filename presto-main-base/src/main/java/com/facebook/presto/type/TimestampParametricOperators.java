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
import com.facebook.presto.common.type.LongTimestamp;
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
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_NULL_FLAG;
import static java.util.Arrays.asList;

/**
 * Comparison, hash, and equality operators for parametric TIMESTAMP type (all precisions 0–12).
 *
 * <p>Short timestamps (precision 0–6) are stored as {@code long}; long timestamps (precision 7–12)
 * are stored as {@link LongTimestamp}.  The {@link com.facebook.presto.metadata.PolymorphicScalarFunction}
 * machinery dispatches to the correct method variant based on the resolved Java type of the argument.
 */
public final class TimestampParametricOperators
{
    private static final TypeSignature TIMESTAMP_SIGNATURE =
            parseTypeSignature("timestamp(p)", ImmutableSet.of("p"));

    public static final SqlScalarFunction TIMESTAMP_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(EQUAL)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .nullableResult(true)
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("equalShort", "equalLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_NOT_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, NOT_EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(NOT_EQUAL)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .nullableResult(true)
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("notEqualShort", "notEqualLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_LESS_THAN_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, LESS_THAN)
                    .signature(SignatureBuilder.builder()
                            .operatorType(LESS_THAN)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("lessThanShort", "lessThanLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_LESS_THAN_OR_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, LESS_THAN_OR_EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(LESS_THAN_OR_EQUAL)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("lessThanOrEqualShort", "lessThanOrEqualLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_GREATER_THAN_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, GREATER_THAN)
                    .signature(SignatureBuilder.builder()
                            .operatorType(GREATER_THAN)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("greaterThanShort", "greaterThanLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_GREATER_THAN_OR_EQUAL_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, GREATER_THAN_OR_EQUAL)
                    .signature(SignatureBuilder.builder()
                            .operatorType(GREATER_THAN_OR_EQUAL)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("greaterThanOrEqualShort", "greaterThanOrEqualLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_BETWEEN_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, BETWEEN)
                    .signature(SignatureBuilder.builder()
                            .operatorType(BETWEEN)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("betweenShort", "betweenLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_HASH_CODE_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, HASH_CODE)
                    .signature(SignatureBuilder.builder()
                            .operatorType(HASH_CODE)
                            .argumentTypes(TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BIGINT))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("hashCodeShort", "hashCodeLong")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_XX_HASH_64_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, XX_HASH_64)
                    .signature(SignatureBuilder.builder()
                            .operatorType(XX_HASH_64)
                            .argumentTypes(TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BIGINT))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("xxHash64Short", "xxHash64Long")))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_IS_DISTINCT_FROM_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, IS_DISTINCT_FROM)
                    .signature(SignatureBuilder.builder()
                            .operatorType(IS_DISTINCT_FROM)
                            .argumentTypes(TIMESTAMP_SIGNATURE, TIMESTAMP_SIGNATURE)
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
                                            asList(Optional.of(LongTimestamp.class), Optional.of(LongTimestamp.class)))
                                    .methodWithExplicitJavaTypes("distinctBlockPositionShort",
                                            asList(Optional.of(long.class), Optional.of(long.class)))))
                    .build();

    public static final SqlScalarFunction TIMESTAMP_INDETERMINATE_OPERATOR =
            SqlScalarFunction.builder(TimestampParametricOperators.class, INDETERMINATE)
                    .signature(SignatureBuilder.builder()
                            .operatorType(INDETERMINATE)
                            .argumentTypes(TIMESTAMP_SIGNATURE)
                            .returnType(parseTypeSignature(BOOLEAN))
                            .build())
                    .deterministic(true)
                    .choice(choice -> choice
                            .argumentProperties(valueTypeArgumentProperty(USE_NULL_FLAG))
                            .implementation(methodsGroup -> methodsGroup
                                    .methods("indeterminateShort", "indeterminateLong")))
                    .build();

    private TimestampParametricOperators() {}

    // -----------------------------------------------------------------------
    // Short timestamp methods (precision 0–6, Java type = long)
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static Boolean equalShort(long left, long right)
    {
        return left == right;
    }

    @UsedByGeneratedCode
    public static Boolean notEqualShort(long left, long right)
    {
        return left != right;
    }

    @UsedByGeneratedCode
    public static boolean lessThanShort(long left, long right)
    {
        return left < right;
    }

    @UsedByGeneratedCode
    public static boolean lessThanOrEqualShort(long left, long right)
    {
        return left <= right;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanShort(long left, long right)
    {
        return left > right;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanOrEqualShort(long left, long right)
    {
        return left >= right;
    }

    @UsedByGeneratedCode
    public static boolean betweenShort(long value, long min, long max)
    {
        return min <= value && value <= max;
    }

    @UsedByGeneratedCode
    public static long hashCodeShort(long value)
    {
        return AbstractLongType.hash(value);
    }

    @UsedByGeneratedCode
    public static long xxHash64Short(long value)
    {
        return XxHash64.hash(value);
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
        return left != right;
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
        return left.getLong(leftPosition) != right.getLong(rightPosition);
    }

    @UsedByGeneratedCode
    public static boolean indeterminateShort(long value, boolean isNull)
    {
        return isNull;
    }

    // -----------------------------------------------------------------------
    // Long timestamp methods (precision 7–12, Java type = LongTimestamp)
    // -----------------------------------------------------------------------

    @UsedByGeneratedCode
    public static Boolean equalLong(LongTimestamp left, LongTimestamp right)
    {
        return left.compareTo(right) == 0;
    }

    @UsedByGeneratedCode
    public static Boolean notEqualLong(LongTimestamp left, LongTimestamp right)
    {
        return left.compareTo(right) != 0;
    }

    @UsedByGeneratedCode
    public static boolean lessThanLong(LongTimestamp left, LongTimestamp right)
    {
        return left.compareTo(right) < 0;
    }

    @UsedByGeneratedCode
    public static boolean lessThanOrEqualLong(LongTimestamp left, LongTimestamp right)
    {
        return left.compareTo(right) <= 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanLong(LongTimestamp left, LongTimestamp right)
    {
        return left.compareTo(right) > 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanOrEqualLong(LongTimestamp left, LongTimestamp right)
    {
        return left.compareTo(right) >= 0;
    }

    @UsedByGeneratedCode
    public static boolean betweenLong(LongTimestamp value, LongTimestamp min, LongTimestamp max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @UsedByGeneratedCode
    public static long hashCodeLong(LongTimestamp value)
    {
        return AbstractLongType.hash(value.getEpochMicros()) * 31 + value.getPicosOfMicro();
    }

    @UsedByGeneratedCode
    public static long xxHash64Long(LongTimestamp value)
    {
        return XxHash64.hash(value.getEpochMicros()) * 31 + value.getPicosOfMicro();
    }

    @UsedByGeneratedCode
    public static boolean distinctLong(LongTimestamp left, boolean leftNull, LongTimestamp right, boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return left.compareTo(right) != 0;
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
        long leftEpochMicros = left.getLong(leftPosition, 0);
        int leftPicosOfMicro = left.getInt(leftPosition);
        long rightEpochMicros = right.getLong(rightPosition, 0);
        int rightPicosOfMicro = right.getInt(rightPosition);
        return leftEpochMicros != rightEpochMicros || leftPicosOfMicro != rightPicosOfMicro;
    }

    @UsedByGeneratedCode
    public static boolean indeterminateLong(LongTimestamp value, boolean isNull)
    {
        return isNull;
    }
}

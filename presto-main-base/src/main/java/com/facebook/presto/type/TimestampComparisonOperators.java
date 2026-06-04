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
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.XxHash64;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static java.util.Arrays.asList;

// Comparison operators for TIMESTAMP(p) where p ≠ 3.
// Each operator has two choices:
//   1. RETURN_NULL_ON_NULL (or USE_NULL_FLAG for distinct/indeterminate): used by the expression
//      interpreter for computed values and short timestamps (p ≤ 6 stored as scaled long).
//   2. BLOCK_AND_POSITION: used in vectorized block processing and handles all precisions including
//      p > 6 (Fixed12ArrayBlock with epochMicros + picosOfMicro).
// Exact-type annotation-based operators in TimestampOperators.java win for p=3 inputs.
public final class TimestampComparisonOperators
{
    private TimestampComparisonOperators() {}

    // TimestampType.getJavaType() returns long.class for all precisions (inherited from AbstractLongType).
    private static final Optional<Class<?>> LONG_TYPE = Optional.of(long.class);

    public static final SqlScalarFunction TIMESTAMP_EQUAL = SqlScalarFunction.builder(TimestampComparisonOperators.class, EQUAL)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(EQUAL)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
                    .returnType(parseTypeSignature(BOOLEAN))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .nullableResult(true)
                    .implementation(mg -> mg
                            .methods("equalShort")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .choice(choice -> choice
                    .nullableResult(true)
                    .argumentProperties(
                            valueTypeArgumentProperty(BLOCK_AND_POSITION),
                            valueTypeArgumentProperty(BLOCK_AND_POSITION))
                    .implementation(mg -> mg
                            .methodWithExplicitJavaTypes("equal", asList(LONG_TYPE, LONG_TYPE))
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_LESS_THAN = SqlScalarFunction.builder(TimestampComparisonOperators.class, LESS_THAN)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(LESS_THAN)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
                    .returnType(parseTypeSignature(BOOLEAN))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("lessThanShort")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .choice(choice -> choice
                    .argumentProperties(
                            valueTypeArgumentProperty(BLOCK_AND_POSITION),
                            valueTypeArgumentProperty(BLOCK_AND_POSITION))
                    .implementation(mg -> mg
                            .methodWithExplicitJavaTypes("lessThan", asList(LONG_TYPE, LONG_TYPE))
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_HASH_CODE = SqlScalarFunction.builder(TimestampComparisonOperators.class, HASH_CODE)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(HASH_CODE)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"))
                    .returnType(parseTypeSignature(BIGINT))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("hashCodeShort")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .choice(choice -> choice
                    .argumentProperties(valueTypeArgumentProperty(BLOCK_AND_POSITION))
                    .implementation(mg -> mg
                            .methodWithExplicitJavaTypes("hashCodeOperator", asList(LONG_TYPE))
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_XX_HASH_64 = SqlScalarFunction.builder(TimestampComparisonOperators.class, XX_HASH_64)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(XX_HASH_64)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"))
                    .returnType(parseTypeSignature(BIGINT))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(mg -> mg
                            .methods("xxHash64Short")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .choice(choice -> choice
                    .argumentProperties(valueTypeArgumentProperty(BLOCK_AND_POSITION))
                    .implementation(mg -> mg
                            .methodWithExplicitJavaTypes("xxHash64", asList(LONG_TYPE))
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_DISTINCT_FROM = SqlScalarFunction.builder(TimestampComparisonOperators.class, IS_DISTINCT_FROM)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(IS_DISTINCT_FROM)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
                    .returnType(parseTypeSignature(BOOLEAN))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .argumentProperties(
                            valueTypeArgumentProperty(USE_NULL_FLAG),
                            valueTypeArgumentProperty(USE_NULL_FLAG))
                    .implementation(mg -> mg
                            .methods("isDistinctFromShort")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .choice(choice -> choice
                    .argumentProperties(
                            valueTypeArgumentProperty(BLOCK_AND_POSITION),
                            valueTypeArgumentProperty(BLOCK_AND_POSITION))
                    .implementation(mg -> mg
                            .methodWithExplicitJavaTypes("isDistinctFrom", asList(LONG_TYPE, LONG_TYPE))
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    public static final SqlScalarFunction TIMESTAMP_INDETERMINATE = SqlScalarFunction.builder(TimestampComparisonOperators.class, INDETERMINATE)
            .signature(SignatureBuilder.builder()
                    .kind(SCALAR)
                    .operatorType(INDETERMINATE)
                    .typeVariableConstraints(withVariadicBound("T", TIMESTAMP))
                    .argumentTypes(parseTypeSignature("T"))
                    .returnType(parseTypeSignature(BOOLEAN))
                    .build())
            .deterministic(true)
            .choice(choice -> choice
                    .argumentProperties(valueTypeArgumentProperty(USE_NULL_FLAG))
                    .implementation(mg -> mg
                            .methods("indeterminateShort")
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .choice(choice -> choice
                    .argumentProperties(valueTypeArgumentProperty(BLOCK_AND_POSITION))
                    .implementation(mg -> mg
                            .methodWithExplicitJavaTypes("indeterminate", asList(LONG_TYPE))
                            .withExtraParameters(ctx -> ImmutableList.of((long) ((TimestampType) ctx.getType("T")).getPrecision()))))
            .build();

    // --- Short-path methods (RETURN_NULL_ON_NULL / USE_NULL_FLAG convention) ---
    // Used by the expression interpreter for computed values and p ≤ 6 timestamps.

    @UsedByGeneratedCode
    public static Boolean equalShort(long left, long right, long precision)
    {
        return left == right;
    }

    @UsedByGeneratedCode
    public static boolean lessThanShort(long left, long right, long precision)
    {
        return left < right;
    }

    @UsedByGeneratedCode
    public static long hashCodeShort(long value, long precision)
    {
        return AbstractLongType.hash(value);
    }

    @UsedByGeneratedCode
    public static long xxHash64Short(long value, long precision)
    {
        return XxHash64.hash(value);
    }

    @UsedByGeneratedCode
    public static boolean isDistinctFromShort(long left, boolean leftIsNull, long right, boolean rightIsNull, long precision)
    {
        if (leftIsNull != rightIsNull) {
            return true;
        }
        if (leftIsNull) {
            return false;
        }
        return left != right;
    }

    @UsedByGeneratedCode
    public static boolean indeterminateShort(long value, boolean isNull, long precision)
    {
        return isNull;
    }

    // --- Block-position methods (BLOCK_AND_POSITION convention) ---
    // Used in vectorized block processing; handles all precisions including p > 6.

    @UsedByGeneratedCode
    public static Boolean equal(Block left, int lp, Block right, int rp, long precision)
    {
        if (left.isNull(lp) || right.isNull(rp)) {
            return null;
        }
        return compare(left, lp, right, rp, createTimestampType((int) precision)) == 0;
    }

    @UsedByGeneratedCode
    public static boolean lessThan(Block left, int lp, Block right, int rp, long precision)
    {
        return compare(left, lp, right, rp, createTimestampType((int) precision)) < 0;
    }

    @UsedByGeneratedCode
    public static long hashCodeOperator(Block block, int pos, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        if (type.isShort()) {
            return AbstractLongType.hash(type.getLong(block, pos));
        }
        long epochMicros = block.getLong(pos, 0);
        int picosOfMicro = block.getInt(pos);
        return AbstractLongType.hash(epochMicros) + 31L * AbstractLongType.hash(picosOfMicro);
    }

    @UsedByGeneratedCode
    public static long xxHash64(Block block, int pos, long precision)
    {
        TimestampType type = createTimestampType((int) precision);
        if (type.isShort()) {
            return XxHash64.hash(type.getLong(block, pos));
        }
        long epochMicros = block.getLong(pos, 0);
        int picosOfMicro = block.getInt(pos);
        return XxHash64.hash(epochMicros) + 31L * XxHash64.hash(picosOfMicro);
    }

    @UsedByGeneratedCode
    public static boolean isDistinctFrom(Block left, int lp, Block right, int rp, long precision)
    {
        if (left.isNull(lp) != right.isNull(rp)) {
            return true;
        }
        if (left.isNull(lp)) {
            return false;
        }
        return compare(left, lp, right, rp, createTimestampType((int) precision)) != 0;
    }

    @UsedByGeneratedCode
    public static boolean indeterminate(Block block, int pos, long precision)
    {
        return block.isNull(pos);
    }

    // For p ≤ 6 (short): reads the epoch-scaled long via type.getLong.
    // For p > 6 (long): reads epochMicros via getLong(pos, 0) and picosOfMicro via getInt(pos)
    // from the Fixed12ArrayBlock, comparing epochMicros first, picosOfMicro as tiebreaker.
    private static int compare(Block left, int lp, Block right, int rp, TimestampType type)
    {
        if (type.isShort()) {
            return Long.compare(type.getLong(left, lp), type.getLong(right, rp));
        }
        long lEpochMicros = left.getLong(lp, 0);
        int lPicos = left.getInt(lp);
        long rEpochMicros = right.getLong(rp, 0);
        int rPicos = right.getInt(rp);
        int c = Long.compare(lEpochMicros, rEpochMicros);
        return c != 0 ? c : Integer.compare(lPicos, rPicos);
    }
}

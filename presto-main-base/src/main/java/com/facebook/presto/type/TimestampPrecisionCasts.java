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
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.function.Signature;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static java.lang.Math.floorDiv;

// Implements CAST between TIMESTAMP(p1) and TIMESTAMP(p2) for short precisions (p ≤ 6).
// Widening zero-fills; narrowing truncates toward negative infinity (floor semantics).
public final class TimestampPrecisionCasts
{
    // Units per second for each short precision level (p=0 through p=6).
    // PRECISION_SCALE[p] = 10^p.
    static final long[] PRECISION_SCALE = {
            1L,           // p=0 (seconds)
            10L,          // p=1
            100L,         // p=2
            1_000L,       // p=3 (milliseconds)
            10_000L,      // p=4
            100_000L,     // p=5
            1_000_000L,   // p=6 (microseconds)
    };

    public static final Signature SIGNATURE = SignatureBuilder.builder()
            .kind(SCALAR)
            .operatorType(CAST)
            .typeVariableConstraints(withVariadicBound("F", TIMESTAMP), withVariadicBound("T", TIMESTAMP))
            .argumentTypes(parseTypeSignature("F"))
            .returnType(parseTypeSignature("T"))
            .build();

    public static final SqlScalarFunction TIMESTAMP_TO_TIMESTAMP_CAST = SqlScalarFunction.builder(TimestampPrecisionCasts.class, CAST)
            .signature(SIGNATURE)
            .deterministic(true)
            .choice(choice -> choice
                    .implementation(methodsGroup -> methodsGroup
                            .methods("shortToShortCast")
                            .withExtraParameters(context -> {
                                TimestampType fromType = (TimestampType) context.getType("F");
                                TimestampType toType = (TimestampType) context.getType("T");
                                return ImmutableList.of((long) fromType.getPrecision(), (long) toType.getPrecision());
                            })))
            .build();

    private TimestampPrecisionCasts() {}

    @UsedByGeneratedCode
    public static long shortToShortCast(long value, long fromPrecision, long toPrecision)
    {
        if (toPrecision == fromPrecision) {
            return value;
        }
        if (toPrecision > fromPrecision) {
            return value * (PRECISION_SCALE[(int) toPrecision] / PRECISION_SCALE[(int) fromPrecision]);
        }
        return floorDiv(value, PRECISION_SCALE[(int) fromPrecision] / PRECISION_SCALE[(int) toPrecision]);
    }
}

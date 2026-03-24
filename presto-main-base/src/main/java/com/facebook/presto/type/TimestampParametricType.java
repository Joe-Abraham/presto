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

import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;

import java.util.List;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implements the parametric {@code TIMESTAMP(p)} type, where {@code p} is the
 * fractional-seconds precision in the range 0–12.
 *
 * <ul>
 *   <li>p = 0..3  → {@link TimestampType#TIMESTAMP} (stored as epoch milliseconds)</li>
 *   <li>p = 4..12 → {@link TimestampType#TIMESTAMP_MICROSECONDS} (stored as epoch microseconds;
 *       values with precision 7–12 are truncated to microsecond precision at read/write time)</li>
 *   <li>no parameter → defaults to p = 3 (millisecond precision)</li>
 * </ul>
 */
public class TimestampParametricType
        implements ParametricType
{
    public static final TimestampParametricType TIMESTAMP_PARAMETRIC_TYPE = new TimestampParametricType();

    /**
     * Maximum supported fractional-seconds precision for TIMESTAMP(p).
     */
    public static final int MAX_PRECISION = 12;

    /**
     * The precision boundary above which microsecond storage is used.
     * Precision 0–3 uses millisecond storage; precision 4–12 uses microsecond storage.
     */
    public static final int MICROSECONDS_PRECISION_BOUNDARY = 3;

    private TimestampParametricType() {}

    @Override
    public String getName()
    {
        return StandardTypes.TIMESTAMP;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            // Default precision is 3 (milliseconds), consistent with SQL standard.
            return TIMESTAMP;
        }

        checkArgument(parameters.size() == 1, "TIMESTAMP type requires exactly one precision parameter, got %s", parameters.size());

        TypeParameter parameter = parameters.get(0);
        checkArgument(parameter.isLongLiteral(), "TIMESTAMP precision must be an integer, got %s", parameter);

        long precision = parameter.getLongLiteral();
        checkArgument(precision >= 0 && precision <= MAX_PRECISION,
                "TIMESTAMP precision must be in the range [0, %s], got %s", MAX_PRECISION, precision);

        if (precision <= MICROSECONDS_PRECISION_BOUNDARY) {
            // Precision 0–3: stored as epoch milliseconds.
            return TIMESTAMP;
        }
        else {
            // Precision 4–12: stored as epoch microseconds.
            // For precision 7–12, values are truncated to microsecond precision (6 significant
            // fractional digits) because the underlying storage is 64-bit epoch microseconds.
            return TIMESTAMP_MICROSECONDS;
        }
    }
}

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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implements the parametric {@code TIMESTAMP(p)} type, where {@code p} is the
 * fractional-seconds precision in the range 0–12.
 *
 * <ul>
 *   <li>No parameter → defaults to p = 3 (SQL standard, millisecond precision)</li>
 *   <li>p = 0..3  → {@link TimestampType#TIMESTAMP} (epoch milliseconds storage)</li>
 *   <li>p = 4..12 → {@link TimestampType#TIMESTAMP_MICROSECONDS} (epoch microseconds storage;
 *       values with precision 7–12 are truncated to microsecond precision at read/write time)</li>
 * </ul>
 */
public class TimestampParametricType
        implements ParametricType
{
    public static final TimestampParametricType TIMESTAMP_PARAMETRIC_TYPE = new TimestampParametricType();

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
            // Default precision is 3 (milliseconds), consistent with the SQL standard.
            return TimestampType.TIMESTAMP;
        }

        checkArgument(parameters.size() == 1,
                "TIMESTAMP type requires exactly one precision parameter, got %s", parameters.size());

        TypeParameter parameter = parameters.get(0);
        checkArgument(parameter.isLongLiteral(),
                "TIMESTAMP precision must be an integer, got %s", parameter);

        long precision = parameter.getLongLiteral();
        checkArgument(precision >= 0 && precision <= TimestampType.MAX_PRECISION,
                "TIMESTAMP precision must be in the range [0, %s], got %s", TimestampType.MAX_PRECISION, precision);

        return TimestampType.createTimestampType((int) precision);
    }
}

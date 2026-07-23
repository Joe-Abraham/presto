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
package com.facebook.presto.common.type;

import com.facebook.presto.common.InvalidFunctionArgumentException;

import java.util.List;

import static com.facebook.presto.common.type.TimestampType.DEFAULT_PRECISION;
import static com.facebook.presto.common.type.TimestampType.MAX_PRECISION;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static java.lang.String.format;

// Resolves "timestamp(p)" type signatures to TimestampType instances.
// Registered alongside the bare TIMESTAMP singleton so the type registry can
// handle both "timestamp" (→ p=3) and "timestamp(p)" (→ any precision in [0,12]).
public class TimestampParametricType
        implements ParametricType
{
    public static final TimestampParametricType TIMESTAMP = new TimestampParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.TIMESTAMP;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return createTimestampType(DEFAULT_PRECISION);
        }
        if (parameters.size() != 1) {
            throw new InvalidFunctionArgumentException(format(
                    "TIMESTAMP requires exactly one precision parameter in the range [0, %d], got %d parameters: %s",
                    MAX_PRECISION, parameters.size(), parameters));
        }
        TypeParameter parameter = parameters.get(0);
        if (!parameter.isLongLiteral()) {
            throw new InvalidFunctionArgumentException(format(
                    "TIMESTAMP precision must be an integer literal in the range [0, %d], got: %s", MAX_PRECISION, parameter));
        }
        // Validate the range before narrowing to int: a long outside int range could otherwise
        // wrap around and silently land inside [0, MAX_PRECISION], bypassing createTimestampType's check.
        long precision = parameter.getLongLiteral();
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new InvalidFunctionArgumentException(format(
                    "TIMESTAMP precision must be in the range [0, %d], got: %d", MAX_PRECISION, precision));
        }
        return createTimestampType((int) precision);
    }
}

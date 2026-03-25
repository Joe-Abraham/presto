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
import com.facebook.presto.spi.PrestoException;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * Parametric type handler for {@code TIMESTAMP(p)} SQL syntax.
 *
 * <p>Supported precisions are 0 (seconds), 3 (milliseconds, the default),
 * 6 (microseconds), and 9 (nanoseconds).  When no precision is supplied the
 * default precision 3 (milliseconds) is used, preserving backward compatibility
 * with plain {@code TIMESTAMP}.
 */
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
            // No precision supplied – default is 3 (milliseconds).
            return TimestampType.TIMESTAMP;
        }
        if (parameters.size() != 1) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "TIMESTAMP type takes at most one parameter, but got " + parameters.size());
        }

        TypeParameter parameter = parameters.get(0);
        if (!parameter.isLongLiteral()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "TIMESTAMP precision must be an integer literal");
        }

        int precision = parameter.getLongLiteral().intValue();
        try {
            return TimestampType.createTimestampType(precision);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage());
        }
    }
}

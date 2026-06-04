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
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;

import java.util.List;

/**
 * Parametric type handler for {@code TIMESTAMP WITH TIME ZONE(p)} where {@code p} is the precision
 * (0..12).
 * <p>
 * When no parameter is specified, defaults to precision 3 (millisecond) for backward compatibility.
 * <p>
 * Precision 0-3: Short timestamps with time zone, stored as a single packed {@code long}.<br>
 * Precision 4-12: Long timestamps with time zone, stored as 12 bytes.
 */
public class TimestampWithTimeZoneParametricType
        implements ParametricType
{
    public static final TimestampWithTimeZoneParametricType TIMESTAMP_WITH_TIME_ZONE =
            new TimestampWithTimeZoneParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        }
        if (parameters.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one parameter for TIMESTAMP WITH TIME ZONE");
        }

        TypeParameter parameter = parameters.get(0);

        if (!parameter.isLongLiteral()) {
            throw new IllegalArgumentException("TIMESTAMP WITH TIME ZONE precision must be a number");
        }

        long precision = parameter.getLongLiteral();

        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid TIMESTAMP WITH TIME ZONE precision " + precision);
        }

        return TimestampWithTimeZoneType.createTimestampWithTimeZoneType((int) precision);
    }
}

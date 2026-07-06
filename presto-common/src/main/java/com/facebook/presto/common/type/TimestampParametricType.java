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

import java.util.List;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;

public class TimestampParametricType
        implements ParametricType
{
    public static final TimestampParametricType TIMESTAMP_PARAMETRIC = new TimestampParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.TIMESTAMP;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        if (parameters.size() != 1 || !parameters.get(0).isLongLiteral()) {
            throw new IllegalArgumentException("Expected exactly one integer parameter for TIMESTAMP type");
        }
        return createTimestampType(parameters.get(0).getLongLiteral().intValue());
    }
}

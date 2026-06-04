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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.TimestampType;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;

/**
 * The precision at which timestamps read from Hive connector sources are exposed to Presto.
 *
 * <p>Hive stores timestamps in ORC and Parquet with nanosecond precision; in RCFile as strings.
 * PrestoDB historically truncated all Hive timestamps to millisecond precision. This enum allows
 * callers to select the actual precision that should be used when reading.
 */
public enum HiveTimestampPrecision
{
    MILLISECONDS(3),
    MICROSECONDS(6),
    NANOSECONDS(9);

    private final int precision;

    HiveTimestampPrecision(int precision)
    {
        this.precision = precision;
    }

    public int getPrecision()
    {
        return precision;
    }

    public TimestampType getTimestampType()
    {
        return createTimestampType(precision);
    }
}

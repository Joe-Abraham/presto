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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.RichColumnDescriptor;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class LongTimestampMicrosColumnReader
        extends AbstractColumnReader
{
    public LongTimestampMicrosColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long utcMicros = valuesReader.readLong();
            if (type instanceof TimestampWithTimeZoneType) {
                type.writeLong(blockBuilder, packDateTimeWithZone(MICROSECONDS.toMillis(utcMicros), UTC_KEY));
            }
            else if (type instanceof TimestampType
                    && ((TimestampType) type).getPrecision() == MICROSECONDS) {
                // TIMESTAMP_MICROSECONDS: return value as-is in µs.
                type.writeLong(blockBuilder, utcMicros);
            }
            else {
                // TIMESTAMP: caller expects ms.
                type.writeLong(blockBuilder, MICROSECONDS.toMillis(utcMicros));
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readLong();
        }
    }
}

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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;

/**
 * Short TIMESTAMP WITH TIME ZONE type (precision 0-3). Values are stored as a single packed
 * {@code long}: epoch milliseconds in bits 63:12, timezone key in bits 11:0.
 * <p>
 * This matches the original single-precision {@link TimestampWithTimeZoneType} storage format
 * and is therefore fully backward compatible.
 */
public final class ShortTimestampWithTimeZoneType
        extends TimestampWithTimeZoneType
{
    ShortTimestampWithTimeZoneType(int precision)
    {
        super(precision, long.class);
        if (precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException("Short TIMESTAMP WITH TIME ZONE precision must be <= " + MAX_SHORT_PRECISION + ": " + precision);
        }
    }

    @Override
    public int getFixedSize()
    {
        return Long.BYTES;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlTimestampWithTimeZone(block.getLong(position));
    }

    @Override
    public long getLong(Block block, int position)
    {
        return block.getLong(position);
    }

    @Override
    public long getLongUnchecked(UncheckedBlock block, int internalPosition)
    {
        return block.getLongUnchecked(internalPosition);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder.writeLong(value).closeEntry();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position)).closeEntry();
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = unpackMillisUtc(leftBlock.getLong(leftPosition));
        long rightValue = unpackMillisUtc(rightBlock.getLong(rightPosition));
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return AbstractLongType.hash(unpackMillisUtc(block.getLong(position)));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = unpackMillisUtc(leftBlock.getLong(leftPosition));
        long rightValue = unpackMillisUtc(rightBlock.getLong(rightPosition));
        return Long.compare(leftValue, rightValue);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new LongArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Long.BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Long.BYTES);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null, positionCount);
    }
}

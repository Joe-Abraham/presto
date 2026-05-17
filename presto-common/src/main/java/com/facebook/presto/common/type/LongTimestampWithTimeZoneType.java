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
import com.facebook.presto.common.block.Fixed12Block;
import com.facebook.presto.common.block.Fixed12BlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.block.Fixed12Block.FIXED12_BYTES;

/**
 * Long TIMESTAMP WITH TIME ZONE type (precision 4-9). Values are stored as 12 bytes
 * using {@link Fixed12Block}:
 * <ul>
 *   <li>First 8 bytes: epoch milliseconds (UTC) as a {@code long}</li>
 *   <li>Last 4 bytes: {@code (nanosOfMilli << 12) | (timeZoneKey & 0xFFF)}</li>
 * </ul>
 *
 * <p>The maximum supported precision is {@value #MAX_PRECISION_TSTZ} (nanoseconds). Precision
 * 10–12 (picoseconds) cannot be represented because the 20-bit {@code nanosOfMilli} field and
 * the 12-bit timezone key together exactly fill the 32-bit packed word, leaving no room for
 * sub-nanosecond digits.
 *
 * @see LongTimestampWithTimeZone
 */
public final class LongTimestampWithTimeZoneType
        extends TimestampWithTimeZoneType
{
    /**
     * Maximum precision supported by this type. Precision 10–12 cannot be stored because
     * the packed 32-bit word ({@code nanosOfMilli << 12 | timeZoneKey}) has no remaining
     * bits for sub-nanosecond values.
     */
    public static final int MAX_PRECISION_TSTZ = 9;

    LongTimestampWithTimeZoneType(int precision)
    {
        super(precision, LongTimestampWithTimeZone.class);
        if (precision <= MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException("Long TIMESTAMP WITH TIME ZONE precision must be > " + MAX_SHORT_PRECISION + ": " + precision);
        }
        if (precision > MAX_PRECISION_TSTZ) {
            throw new IllegalArgumentException("Long TIMESTAMP WITH TIME ZONE precision must be <= " + MAX_PRECISION_TSTZ + ": " + precision);
        }
    }

    @Override
    public int getFixedSize()
    {
        return FIXED12_BYTES;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return getObject(block, position);
    }

    /**
     * Reads a {@link LongTimestampWithTimeZone} from a block at the given position.
     */
    public LongTimestampWithTimeZone getObject(Block block, int position)
    {
        long epochMillis;
        int packed;
        if (block instanceof Fixed12Block) {
            Fixed12Block fixed12Block = (Fixed12Block) block;
            epochMillis = fixed12Block.getFixed12First(position);
            packed = fixed12Block.getFixed12Second(position);
        }
        else {
            epochMillis = block.getLong(position, 0);
            packed = block.getInt(position);
        }
        int nanosOfMilli = LongTimestampWithTimeZone.unpackNanosOfMilli(packed);
        short timeZoneKey = LongTimestampWithTimeZone.unpackTimeZoneKey(packed);
        return new LongTimestampWithTimeZone(epochMillis, nanosOfMilli, timeZoneKey);
    }

    /**
     * Writes a {@link LongTimestampWithTimeZone} value to a block builder.
     */
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LongTimestampWithTimeZone tstz = (LongTimestampWithTimeZone) value;
        int packed = LongTimestampWithTimeZone.packFraction(tstz.getNanosOfMilli(), tstz.getTimeZoneKey().getKey());
        if (blockBuilder instanceof Fixed12BlockBuilder) {
            ((Fixed12BlockBuilder) blockBuilder).writeFixed12(tstz.getEpochMillis(), packed);
        }
        else {
            blockBuilder.writeLong(tstz.getEpochMillis());
            blockBuilder.writeInt(packed);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            LongTimestampWithTimeZone value = getObject(block, position);
            writeObject(blockBuilder, value);
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        LongTimestampWithTimeZone left = getObject(leftBlock, leftPosition);
        LongTimestampWithTimeZone right = getObject(rightBlock, rightPosition);
        // Equality is based on the UTC instant (epochMillis + nanosOfMilli), timezone is irrelevant
        return left.getEpochMillis() == right.getEpochMillis() &&
                left.getNanosOfMilli() == right.getNanosOfMilli();
    }

    @Override
    public long hash(Block block, int position)
    {
        LongTimestampWithTimeZone value = getObject(block, position);
        return AbstractLongType.hash(value.getEpochMillis()) ^ value.getNanosOfMilli();
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        LongTimestampWithTimeZone left = getObject(leftBlock, leftPosition);
        LongTimestampWithTimeZone right = getObject(rightBlock, rightPosition);
        return left.compareTo(right);
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
        return new Fixed12BlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / FIXED12_BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, FIXED12_BYTES);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new Fixed12BlockBuilder(null, positionCount);
    }
}

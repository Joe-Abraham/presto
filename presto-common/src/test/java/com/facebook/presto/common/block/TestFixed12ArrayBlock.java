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
package com.facebook.presto.common.block;

import com.facebook.presto.common.type.LongTimestamp;
import com.facebook.presto.common.type.TimestampType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestFixed12ArrayBlock
{
    @Test
    public void testSinglePositionRoundTrip()
    {
        long epochMicros = 1_000_000_000L;
        int picosOfMicro = 500_000;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 1);
        assertEquals(block.getLong(0, 0), epochMicros);
        assertEquals(block.getInt(0), picosOfMicro);
    }

    @Test
    public void testNegativeEpochMicros()
    {
        long epochMicros = -1_000_000L;
        int picosOfMicro = 999_999;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0, 0), epochMicros);
        assertEquals(block.getInt(0), picosOfMicro);
    }

    @Test
    public void testMultiplePositions()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(100L).writeInt(0).closeEntry();
        builder.writeLong(200L).writeInt(999_999).closeEntry();
        builder.writeLong(-300L).writeInt(500_000).closeEntry();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 3);
        assertEquals(block.getLong(0, 0), 100L);
        assertEquals(block.getInt(0), 0);
        assertEquals(block.getLong(1, 0), 200L);
        assertEquals(block.getInt(1), 999_999);
        assertEquals(block.getLong(2, 0), -300L);
        assertEquals(block.getInt(2), 500_000);
    }

    @Test
    public void testNullPosition()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(100L).writeInt(0).closeEntry();
        builder.appendNull();

        Block block = builder.build();
        assertEquals(block.getPositionCount(), 2);
        assertFalse(block.isNull(0));
        assertTrue(block.isNull(1));
    }

    @Test
    public void testSizeInBytes()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 4);
        for (int i = 0; i < 4; i++) {
            builder.writeLong((long) i * 1000).writeInt(i).closeEntry();
        }
        Block block = builder.build();
        assertEquals(block.getSizeInBytes(), Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION * 4L);
    }

    @Test
    public void testTimestampTypeCreateBlockBuilderForLongPrecision()
    {
        TimestampType longType = createTimestampType(7);
        assertFalse(longType.isShort());
        BlockBuilder builder = longType.createBlockBuilder(null, 10);
        assertNotNull(builder);
        assertTrue(builder instanceof Fixed12ArrayBlockBuilder,
                "Expected Fixed12ArrayBlockBuilder for p=7, got: " + builder.getClass().getSimpleName());
    }

    @Test
    public void testTimestampTypeCreateBlockBuilderForShortPrecision()
    {
        TimestampType shortType = createTimestampType(6);
        assertTrue(shortType.isShort());
        BlockBuilder builder = shortType.createBlockBuilder(null, 10);
        assertNotNull(builder);
        assertTrue(builder instanceof LongArrayBlockBuilder,
                "Expected LongArrayBlockBuilder for p=6, got: " + builder.getClass().getSimpleName());
    }

    @Test
    public void testEncodingRoundTrip()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(1_000_000_000L).writeInt(500_000).closeEntry();
        builder.appendNull();
        builder.writeLong(-999_999L).writeInt(999_999).closeEntry();
        Block original = builder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(256);
        Fixed12ArrayBlockEncoding encoding = new Fixed12ArrayBlockEncoding();
        encoding.writeBlock(null, sliceOutput, original);

        SliceInput sliceInput = sliceOutput.slice().getInput();
        Block decoded = encoding.readBlock(null, sliceInput);

        assertEquals(decoded.getPositionCount(), 3);
        assertFalse(decoded.isNull(0));
        assertEquals(decoded.getLong(0, 0), 1_000_000_000L);
        assertEquals(decoded.getInt(0), 500_000);
        assertTrue(decoded.isNull(1));
        assertFalse(decoded.isNull(2));
        assertEquals(decoded.getLong(2, 0), -999_999L);
        assertEquals(decoded.getInt(2), 999_999);
    }

    @Test
    public void testExtremeNegativeEpoch()
    {
        long epochMicros = Long.MIN_VALUE;
        int picosOfMicro = 0;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0, 0), epochMicros);
        assertEquals(block.getInt(0), picosOfMicro);
    }

    @Test
    public void testGetLongNoOffset()
    {
        long epochMicros = 1_234_567_890L;
        int picosOfMicro = 123_456;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0), epochMicros);
        assertEquals(block.getLong(0, 0), epochMicros);
    }

    @Test
    public void testBuilderGetLongNoOffset()
    {
        long epochMicros = -999_000L;
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(0).closeEntry();

        assertEquals(builder.getLong(0), epochMicros);
        assertEquals(builder.getLong(0, 0), epochMicros);
    }

    @Test
    public void testAppendToLongPrecision()
    {
        TimestampType longType = createTimestampType(9);

        Fixed12ArrayBlockBuilder source = new Fixed12ArrayBlockBuilder(null, 2);
        source.writeLong(1_000_000L).writeInt(500_000).closeEntry();
        source.appendNull();
        Block sourceBlock = source.build();

        BlockBuilder dest = longType.createBlockBuilder(null, 2);
        longType.appendTo(sourceBlock, 0, dest);
        longType.appendTo(sourceBlock, 1, dest);
        Block destBlock = dest.build();

        assertEquals(destBlock.getPositionCount(), 2);
        assertFalse(destBlock.isNull(0));
        assertEquals(destBlock.getLong(0, 0), 1_000_000L);
        assertEquals(destBlock.getInt(0), 500_000);
        assertTrue(destBlock.isNull(1));
    }

    @Test
    public void testWriteLongTimestampRoundTrip()
    {
        TimestampType longType = createTimestampType(9);
        LongTimestamp original = new LongTimestamp(9_876_543_210L, 999_999);

        BlockBuilder builder = longType.createBlockBuilder(null, 1);
        longType.writeLongTimestamp(builder, original);
        Block block = builder.build();

        LongTimestamp read = longType.getLongTimestamp(block, 0);
        assertEquals(read, original);
    }

    @Test
    public void testCompareTo()
    {
        TimestampType longType = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(100L).writeInt(0).closeEntry();
        builder.writeLong(100L).writeInt(500_000).closeEntry();
        builder.writeLong(200L).writeInt(0).closeEntry();
        Block block = builder.build();

        // equal values
        assertEquals(longType.compareTo(block, 0, block, 0), 0);
        // same epochMicros, different picosOfMicro
        assertTrue(longType.compareTo(block, 0, block, 1) < 0);
        assertTrue(longType.compareTo(block, 1, block, 0) > 0);
        // different epochMicros
        assertTrue(longType.compareTo(block, 0, block, 2) < 0);
        assertTrue(longType.compareTo(block, 2, block, 0) > 0);
    }

    @Test
    public void testEqualTo()
    {
        TimestampType longType = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(100L).writeInt(500_000).closeEntry();
        builder.writeLong(100L).writeInt(500_000).closeEntry();
        builder.writeLong(100L).writeInt(999_999).closeEntry();
        Block block = builder.build();

        assertTrue(longType.equalTo(block, 0, block, 0), "value must equal itself");
        assertTrue(longType.equalTo(block, 0, block, 1), "identical values must be equal");
        assertFalse(longType.equalTo(block, 0, block, 2), "same epochMicros but different picosOfMicro must not be equal");
    }

    @Test
    public void testHashEqualsContract()
    {
        TimestampType longType = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(42L).writeInt(7).closeEntry();
        builder.writeLong(42L).writeInt(7).closeEntry();
        Block block = builder.build();

        assertTrue(longType.equalTo(block, 0, block, 1));
        assertEquals(longType.hash(block, 0), longType.hash(block, 1),
                "equal values must have the same hash");
    }

    @Test
    public void testHashDifferentiatesByPicosOfMicro()
    {
        // Guards against a future bug where hash() ignores the picosOfMicro field.
        TimestampType nanos = createTimestampType(9);

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 2);
        builder.writeLong(100L).writeInt(0).closeEntry();
        builder.writeLong(100L).writeInt(1).closeEntry();
        Block block = builder.build();

        assertNotEquals(nanos.hash(block, 0), nanos.hash(block, 1),
                "same epochMicros but different picosOfMicro must produce different hashes");
    }

    @Test
    public void testGetRegionWithNonZeroOffset()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 5);
        for (int i = 0; i < 5; i++) {
            builder.writeLong((long) (i + 1) * 100).writeInt(i * 10).closeEntry();
        }
        Block block = builder.build();

        Block region1 = block.getRegion(1, 4);
        assertEquals(region1.getPositionCount(), 4);
        assertEquals(region1.getLong(0, 0), 200L);
        assertEquals(region1.getInt(0), 10);

        Block region2 = region1.getRegion(1, 2);
        assertEquals(region2.getPositionCount(), 2);
        assertEquals(region2.getLong(0, 0), 300L);
        assertEquals(region2.getInt(0), 20);
        assertEquals(region2.getLong(1, 0), 400L);
        assertEquals(region2.getInt(1), 30);
    }

    @Test
    public void testCopyRegionWithNonZeroOffset()
    {
        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 5);
        for (int i = 0; i < 5; i++) {
            builder.writeLong((long) (i + 1) * 100).writeInt(i * 10).closeEntry();
        }
        Block block = builder.build();

        Block region = block.getRegion(2, 3);
        Block copy = region.copyRegion(0, 2);

        assertEquals(copy.getPositionCount(), 2);
        assertEquals(copy.getLong(0, 0), 300L);
        assertEquals(copy.getInt(0), 20);
        assertEquals(copy.getLong(1, 0), 400L);
        assertEquals(copy.getInt(1), 30);
        assertEquals(copy.getLong(0, 0), region.getLong(0, 0));
    }

    @Test
    public void testEncodingRoundTripViaSerde()
    {
        BlockEncodingSerde serde = new TestingBlockEncodingSerde();

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 3);
        builder.writeLong(1_000_000_000L).writeInt(500_000).closeEntry();
        builder.appendNull();
        builder.writeLong(-999_999L).writeInt(999_999).closeEntry();
        Block original = builder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(256);
        serde.writeBlock(sliceOutput, original);
        Block decoded = serde.readBlock(sliceOutput.slice().getInput());

        assertEquals(decoded.getPositionCount(), 3);
        assertFalse(decoded.isNull(0));
        assertEquals(decoded.getLong(0, 0), 1_000_000_000L);
        assertEquals(decoded.getInt(0), 500_000);
        assertTrue(decoded.isNull(1));
        assertFalse(decoded.isNull(2));
        assertEquals(decoded.getLong(2, 0), -999_999L);
        assertEquals(decoded.getInt(2), 999_999);
    }
}

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

import com.facebook.presto.common.type.TimestampType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
        // Pre-1970: epochMicros is negative
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
        // 12 bytes data + 1 byte null flag per position = 13 bytes per position
        assertEquals(block.getSizeInBytes(), Fixed12ArrayBlock.SIZE_IN_BYTES_PER_POSITION * 4L);
    }

    @Test
    public void testTimestampTypeCreateBlockBuilderForLongPrecision()
    {
        // p > 6 should return Fixed12ArrayBlockBuilder
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
        // p <= 6 should return LongArrayBlockBuilder (the default from AbstractLongType)
        TimestampType shortType = createTimestampType(6);
        assertTrue(shortType.isShort());
        BlockBuilder builder = shortType.createBlockBuilder(null, 10);
        assertNotNull(builder);
        assertTrue(builder instanceof LongArrayBlockBuilder,
                "Expected LongArrayBlockBuilder for p=6, got: " + builder.getClass().getSimpleName());
    }

    @Test
    public void testExtremeNegativeEpoch()
    {
        // Long.MIN_VALUE is a valid epochMicros (no restriction)
        long epochMicros = Long.MIN_VALUE;
        int picosOfMicro = 0;

        Fixed12ArrayBlockBuilder builder = new Fixed12ArrayBlockBuilder(null, 1);
        builder.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();

        Block block = builder.build();
        assertEquals(block.getLong(0, 0), epochMicros);
        assertEquals(block.getInt(0), picosOfMicro);
    }
}

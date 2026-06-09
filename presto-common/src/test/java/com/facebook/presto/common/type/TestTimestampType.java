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
import com.facebook.presto.common.block.Fixed12ArrayBlock;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTimestampType
{
    @Test
    public void testInterningReturnsSameReference()
    {
        for (int p = 0; p <= TimestampType.MAX_PRECISION; p++) {
            assertSame(createTimestampType(p), createTimestampType(p),
                    "createTimestampType(" + p + ") must return same reference on repeated calls");
        }
    }

    @Test
    public void testAllInstancesPreAllocated()
    {
        for (int p = 0; p <= TimestampType.MAX_PRECISION; p++) {
            assertNotNull(createTimestampType(p), "Instance for precision " + p + " must be pre-allocated");
        }
    }

    @Test
    public void testTimestampConstantBackwardCompatibility()
    {
        assertSame(TIMESTAMP, createTimestampType(3), "TIMESTAMP must be same reference as createTimestampType(3)");
    }

    @Test
    public void testIsShortBoundary()
    {
        for (int p = 0; p <= TimestampType.MAX_SHORT_PRECISION; p++) {
            assertTrue(createTimestampType(p).isShort(), "isShort() must return true for p=" + p);
        }
        for (int p = TimestampType.MAX_SHORT_PRECISION + 1; p <= TimestampType.MAX_PRECISION; p++) {
            assertFalse(createTimestampType(p).isShort(), "isShort() must return false for p=" + p);
        }
    }

    @Test
    public void testInvalidPrecisionThrows()
    {
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(-1));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(13));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(Integer.MIN_VALUE));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(Integer.MAX_VALUE));
    }

    @Test
    public void testInvalidPrecisionErrorMessage()
    {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createTimestampType(-1));
        assertTrue(e.getMessage().contains("-1"), "Error message must contain the invalid value");

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> createTimestampType(13));
        assertTrue(e2.getMessage().contains("13"), "Error message must contain the invalid value");
    }

    @Test
    public void testTimestampMicrosecondsIsInterned()
    {
        assertSame(TIMESTAMP_MICROSECONDS, createTimestampType(6),
                "TIMESTAMP_MICROSECONDS must be same reference as createTimestampType(6)");
    }

    @Test
    public void testReferenceEqualityIsTypeEquality()
    {
        for (int p1 = 0; p1 <= TimestampType.MAX_PRECISION; p1++) {
            for (int p2 = 0; p2 <= TimestampType.MAX_PRECISION; p2++) {
                if (p1 == p2) {
                    assertEquals(createTimestampType(p1), createTimestampType(p2));
                }
                else {
                    assertFalse(createTimestampType(p1).equals(createTimestampType(p2)),
                            "Types with different precision must not be equal");
                }
            }
        }
    }

    @Test
    public void testEpochComponentsMillis()
    {
        TimestampType ts = createTimestampType(3);
        assertEquals(ts.getEpochSecond(1_000L), 1L);
        assertEquals(ts.getNanos(1_000L), 0);
        assertEquals(ts.getEpochSecond(1_500L), 1L);
        assertEquals(ts.getNanos(1_500L), 500_000_000);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_000_000);
    }

    @Test
    public void testToEpochMicros()
    {
        assertEquals(createTimestampType(3).toEpochMicros(1_500L), 1_500_000L);
        assertEquals(createTimestampType(3).toEpochMicros(0L), 0L);
        assertEquals(createTimestampType(3).toEpochMicros(-1L), -1_000L);
        assertEquals(createTimestampType(6).toEpochMicros(1_500_000L), 1_500_000L);
        assertEquals(createTimestampType(6).toEpochMicros(-1L), -1L);
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(7).toEpochMicros(0L));
    }

    @Test
    public void testFromEpochComponents()
    {
        TimestampType millis = createTimestampType(3);
        assertEquals(millis.fromEpochComponents(1L, 500_000_000), 1_500L);
        assertEquals(millis.fromEpochComponents(0L, 0), 0L);
        assertEquals(millis.fromEpochComponents(-1L, 999_000_000), -1L);

        TimestampType micros = createTimestampType(6);
        assertEquals(micros.fromEpochComponents(1L, 500_000_000), 1_500_000L);

        expectThrows(IllegalArgumentException.class, () -> millis.fromEpochComponents(0L, -1));
        expectThrows(IllegalArgumentException.class, () -> millis.fromEpochComponents(0L, 1_000_000_000));
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(7).fromEpochComponents(0L, 0));
    }

    @Test
    public void testEpochComponentsMicros()
    {
        TimestampType ts = createTimestampType(6);
        assertEquals(ts.getEpochSecond(1_000_000L), 1L);
        assertEquals(ts.getNanos(1_000_000L), 0);
        assertEquals(ts.getEpochSecond(1_500_000L), 1L);
        assertEquals(ts.getNanos(1_500_000L), 500_000_000);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_999_000);
    }

    @Test
    public void testToEpochMillis()
    {
        assertEquals(createTimestampType(3).toEpochMillis(1_500L), 1_500L);
        assertEquals(createTimestampType(6).toEpochMillis(1_500_000L), 1_500L);
        assertEquals(createTimestampType(3).toEpochMillis(-1L), -1L);
        assertEquals(createTimestampType(6).toEpochMillis(-1L), -1L);
        assertEquals(createTimestampType(6).toEpochMillis(-1_000L), -1L);
    }

    @Test
    public void testEpochComponentsSeconds()
    {
        TimestampType ts = createTimestampType(0);
        assertEquals(ts.getEpochSecond(5L), 5L);
        assertEquals(ts.getNanos(5L), 0);
        assertEquals(ts.toEpochMillis(5L), 5_000L);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 0);
        assertEquals(ts.toEpochMillis(-1L), -1_000L);
    }

    @Test
    public void testEpochComponentsIntermediatePrecision()
    {
        // p=4: unit = 100µs (1/10,000 of a second)
        TimestampType ts = createTimestampType(4);
        assertEquals(ts.getEpochSecond(10_000L), 1L);
        assertEquals(ts.getNanos(10_000L), 0);
        assertEquals(ts.getEpochSecond(15_000L), 1L);
        assertEquals(ts.getNanos(15_000L), 500_000_000);
        assertEquals(ts.toEpochMillis(10_000L), 1_000L);
        assertEquals(ts.toEpochMillis(15_000L), 1_500L);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_900_000);
    }

    @Test
    public void testEpochComponentsNanos()
    {
        TimestampType ts = createTimestampType(9);
        assertEquals(ts.getEpochSecond(1_000_000_000L), 1L);
        assertEquals(ts.getNanos(1_000_000_000L), 0);
        assertEquals(ts.getEpochSecond(1_500_000_000L), 1L);
        assertEquals(ts.getNanos(1_500_000_000L), 500_000_000);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_999_999);
    }

    @Test
    public void testGetObjectValueThrowsForUnsupportedShortPrecisions()
    {
        // Build a non-null block so the precision check is reached (null position returns early).
        BlockBuilder builder = TIMESTAMP.createBlockBuilder(null, 1);
        TIMESTAMP.writeLong(builder, 1_000L);
        Block block = builder.build();

        // p=1 and p=4 are short (isShort()==true) but getObjectValue is not yet supported for them.
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(1).getObjectValue(null, block, 0));
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(4).getObjectValue(null, block, 0));
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(7).getObjectValue(null, block, 0));
    }

    @Test
    public void testPreEpochFloorCorrectness()
    {
        // Regression guard: the prior implementation used TimeUnit.toSeconds() (truncation toward
        // zero), which returned 0 for getEpochSecond on any timestamp in (-1s, 0). floorDiv
        // correctly returns -1.
        TimestampType millis = createTimestampType(3);
        assertEquals(millis.getEpochSecond(-1L), -1L);   // was 0 with old truncation
        assertEquals(millis.getNanos(-1L), 999_000_000); // was 0 - wrong sub-second component

        TimestampType micros = createTimestampType(6);
        assertEquals(micros.getEpochSecond(-1L), -1L);
        assertEquals(micros.getNanos(-1L), 999_999_000);
    }

    @Test
    public void testRoundTripFromEpochComponents()
    {
        // fromEpochComponents(getEpochSecond(v), getNanos(v)) must reconstruct v exactly.
        // IcebergPageSink relies on this invariant when converting legacy-timezone timestamps.
        long[] milliValues = {
                0L,
                1L,
                1_000L,
                1_500L,
                999L,
                -1L,
                -1_000L,
                -1_001L,
                -1_500L,
                Long.MAX_VALUE / 1_000 * 1_000,  // large positive aligned to a whole second
        };
        TimestampType millis = createTimestampType(3);
        for (long v : milliValues) {
            assertEquals(millis.fromEpochComponents(millis.getEpochSecond(v), (int) millis.getNanos(v)), v,
                    "round-trip failed for p=3, v=" + v);
        }

        long[] microValues = {
                0L,
                1L,
                1_000_000L,
                1_500_000L,
                999_999L,
                -1L,
                -1_000_000L,
                -1_000_001L,
                -1_500_000L,
        };
        TimestampType micros = createTimestampType(6);
        for (long v : microValues) {
            assertEquals(micros.fromEpochComponents(micros.getEpochSecond(v), (int) micros.getNanos(v)), v,
                    "round-trip failed for p=6, v=" + v);
        }
    }

    @Test
    public void testEpochComponentsPicos()
    {
        TimestampType ts = createTimestampType(12);
        assertEquals(ts.getEpochSecond(1_000_000_000_000L), 1L);
        assertEquals(ts.getNanos(1_000_000_000_000L), 0);
        // 500ps is below nanosecond resolution — getNanos rounds down to 0
        assertEquals(ts.getEpochSecond(1_000_000_000_500L), 1L);
        assertEquals(ts.getNanos(1_000_000_000_500L), 0);
        assertEquals(ts.getEpochSecond(1_500_000_000_000L), 1L);
        assertEquals(ts.getNanos(1_500_000_000_000L), 500_000_000);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_999_999);
    }

    @Test
    public void testGetFixedSize()
    {
        for (int p = 0; p <= TimestampType.MAX_SHORT_PRECISION; p++) {
            assertEquals(createTimestampType(p).getFixedSize(), Long.BYTES,
                    "short precision p=" + p + " must report Long.BYTES");
        }
        for (int p = TimestampType.MAX_SHORT_PRECISION + 1; p <= TimestampType.MAX_PRECISION; p++) {
            assertEquals(createTimestampType(p).getFixedSize(), Fixed12ArrayBlock.FIXED12_BYTES,
                    "long precision p=" + p + " must report FIXED12_BYTES");
        }
    }

    @Test
    public void testWriteLongThrowsForLongPrecision()
    {
        TimestampType longType = createTimestampType(9);
        BlockBuilder builder = longType.createBlockBuilder(null, 1);
        expectThrows(UnsupportedOperationException.class, () -> longType.writeLong(builder, 0L));
    }

    @Test
    public void testWriteLongWorksForShortPrecision()
    {
        TimestampType millis = createTimestampType(3);
        BlockBuilder builder = millis.createBlockBuilder(null, 1);
        millis.writeLong(builder, 1_500L);
        Block block = builder.build();
        assertEquals(block.getLong(0), 1_500L);
    }

    @Test
    public void testWriteLongTimestampAndGetLongTimestamp()
    {
        TimestampType nanos = createTimestampType(9);
        LongTimestamp ts = new LongTimestamp(1_000_000L, 999_999);

        BlockBuilder builder = nanos.createBlockBuilder(null, 1);
        nanos.writeLongTimestamp(builder, ts);
        Block block = builder.build();

        LongTimestamp read = nanos.getLongTimestamp(block, 0);
        assertEquals(read.getEpochMicros(), 1_000_000L);
        assertEquals(read.getPicosOfMicro(), 999_999);
    }

    @Test
    public void testWriteLongTimestampThrowsForShortPrecision()
    {
        TimestampType millis = createTimestampType(3);
        BlockBuilder builder = millis.createBlockBuilder(null, 1);
        expectThrows(UnsupportedOperationException.class,
                () -> millis.writeLongTimestamp(builder, new LongTimestamp(0L, 0)));
    }

    @Test
    public void testGetLongTimestampThrowsForShortPrecision()
    {
        TimestampType millis = createTimestampType(3);
        BlockBuilder builder = millis.createBlockBuilder(null, 1);
        millis.writeLong(builder, 1_000L);
        Block block = builder.build();
        expectThrows(UnsupportedOperationException.class, () -> millis.getLongTimestamp(block, 0));
    }

    @Test
    public void testAppendToLongPrecision()
    {
        TimestampType nanos = createTimestampType(9);
        BlockBuilder source = nanos.createBlockBuilder(null, 2);
        nanos.writeLongTimestamp(source, new LongTimestamp(5_000_000L, 123_456));
        source.appendNull();
        Block sourceBlock = source.build();

        BlockBuilder dest = nanos.createBlockBuilder(null, 2);
        nanos.appendTo(sourceBlock, 0, dest);
        nanos.appendTo(sourceBlock, 1, dest);
        Block destBlock = dest.build();

        assertFalse(destBlock.isNull(0));
        assertEquals(destBlock.getLong(0, 0), 5_000_000L);
        assertEquals(destBlock.getInt(0), 123_456);
        assertTrue(destBlock.isNull(1));
    }

    @Test
    public void testCompareToLongPrecision()
    {
        TimestampType nanos = createTimestampType(9);
        BlockBuilder builder = nanos.createBlockBuilder(null, 3);
        nanos.writeLongTimestamp(builder, new LongTimestamp(100L, 0));
        nanos.writeLongTimestamp(builder, new LongTimestamp(100L, 500_000));
        nanos.writeLongTimestamp(builder, new LongTimestamp(200L, 0));
        Block block = builder.build();

        assertEquals(nanos.compareTo(block, 0, block, 0), 0);
        assertTrue(nanos.compareTo(block, 0, block, 1) < 0);
        assertTrue(nanos.compareTo(block, 1, block, 0) > 0);
        assertTrue(nanos.compareTo(block, 0, block, 2) < 0);
        assertTrue(nanos.compareTo(block, 2, block, 0) > 0);
    }

    @Test
    public void testEqualToLongPrecision()
    {
        TimestampType nanos = createTimestampType(9);
        BlockBuilder builder = nanos.createBlockBuilder(null, 2);
        nanos.writeLongTimestamp(builder, new LongTimestamp(100L, 500_000));
        nanos.writeLongTimestamp(builder, new LongTimestamp(100L, 500_000));
        Block block = builder.build();

        assertTrue(nanos.equalTo(block, 0, block, 1));
        assertTrue(nanos.equalTo(block, 0, block, 0));
    }

    @Test
    public void testHashLongPrecision()
    {
        // hash must not throw for long-precision blocks
        TimestampType nanos = createTimestampType(9);
        BlockBuilder builder = nanos.createBlockBuilder(null, 1);
        nanos.writeLongTimestamp(builder, new LongTimestamp(42L, 7));
        Block block = builder.build();

        long h1 = nanos.hash(block, 0);
        long h2 = nanos.hash(block, 0);
        assertEquals(h1, h2);
    }
}

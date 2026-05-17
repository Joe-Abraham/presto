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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFixed12BlockEncoding
{
    private final Fixed12BlockEncoding encoding = new Fixed12BlockEncoding();
    private final TestingBlockEncodingSerde serde = new TestingBlockEncodingSerde();

    @Test
    public void testEncodingName()
    {
        assertEquals(encoding.getName(), "FIXED12");
    }

    @Test
    public void testRoundTripValues()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 5);
        builder.writeFixed12(0L, 0);
        builder.writeFixed12(Long.MAX_VALUE, Integer.MAX_VALUE);
        builder.writeFixed12(Long.MIN_VALUE, 0);
        builder.writeFixed12(123456789L, 999_999);
        builder.writeFixed12(-1L, 1);
        Block original = builder.build();

        Block roundTripped = roundTrip(original);

        assertEquals(roundTripped.getPositionCount(), 5);
        assertFalse(roundTripped.isNull(0));
        assertFalse(roundTripped.isNull(1));
        assertFalse(roundTripped.isNull(2));

        Fixed12Block result = (Fixed12Block) roundTripped;
        assertEquals(result.getFixed12First(0), 0L);
        assertEquals(result.getFixed12Second(0), 0);
        assertEquals(result.getFixed12First(1), Long.MAX_VALUE);
        assertEquals(result.getFixed12Second(1), Integer.MAX_VALUE);
        assertEquals(result.getFixed12First(2), Long.MIN_VALUE);
        assertEquals(result.getFixed12Second(2), 0);
        assertEquals(result.getFixed12First(3), 123456789L);
        assertEquals(result.getFixed12Second(3), 999_999);
        assertEquals(result.getFixed12First(4), -1L);
        assertEquals(result.getFixed12Second(4), 1);
    }

    @Test
    public void testRoundTripWithNulls()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 4);
        builder.writeFixed12(100L, 10);
        builder.appendNull();
        builder.writeFixed12(200L, 20);
        builder.appendNull();
        Block original = builder.build();

        Block roundTripped = roundTrip(original);

        assertEquals(roundTripped.getPositionCount(), 4);
        assertFalse(roundTripped.isNull(0));
        assertTrue(roundTripped.isNull(1));
        assertFalse(roundTripped.isNull(2));
        assertTrue(roundTripped.isNull(3));

        Fixed12Block result = (Fixed12Block) roundTripped;
        assertEquals(result.getFixed12First(0), 100L);
        assertEquals(result.getFixed12Second(0), 10);
        assertEquals(result.getFixed12First(2), 200L);
        assertEquals(result.getFixed12Second(2), 20);
    }

    @Test
    public void testRoundTripEmpty()
    {
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, 0);
        Block original = builder.build();

        Block roundTripped = roundTrip(original);
        assertEquals(roundTripped.getPositionCount(), 0);
    }

    private Block roundTrip(Block block)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        encoding.writeBlock(serde, output, block);
        SliceInput input = output.slice().getInput();
        return encoding.readBlock(serde, input);
    }
}

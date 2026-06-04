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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestTimestampComparisonOperators
        extends AbstractTestFunctions
{
    public TestTimestampComparisonOperators()
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .build());
    }

    // Build a Fixed12 block holding one non-null TIMESTAMP(9) value.
    private static Block longBlock(long epochMicros, int picosOfMicro)
    {
        BlockBuilder bb = createTimestampType(9).createBlockBuilder(null, 1);
        bb.writeLong(epochMicros).writeInt(picosOfMicro).closeEntry();
        return bb.build();
    }

    private static Block nullLongBlock()
    {
        BlockBuilder bb = createTimestampType(9).createBlockBuilder(null, 1);
        bb.appendNull();
        return bb.build();
    }

    // --- EQUAL p=6 (SQL level) ---

    @Test
    public void testEqualP6EqualValues()
    {
        assertFunction(
                "CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6)) = CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6))",
                BOOLEAN,
                true);
    }

    @Test
    public void testEqualP6DifferentValues()
    {
        assertFunction(
                "CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6)) = CAST(TIMESTAMP '2024-01-01 00:00:00.001' AS TIMESTAMP(6))",
                BOOLEAN,
                false);
    }

    // --- EQUAL p=9 (direct method call — SQL literals max out at p=6) ---

    @Test
    public void testEqualP9PicosDiffer()
    {
        long epochMicros = 1_000_000L;
        assertEquals(
                TimestampComparisonOperators.equal(longBlock(epochMicros, 500), 0, longBlock(epochMicros, 501), 0, 9L),
                Boolean.FALSE);
    }

    @Test
    public void testEqualP9SameValue()
    {
        long epochMicros = 1_000_000L;
        assertEquals(
                TimestampComparisonOperators.equal(longBlock(epochMicros, 500), 0, longBlock(epochMicros, 500), 0, 9L),
                Boolean.TRUE);
    }

    @Test
    public void testEqualNullReturnsNull()
    {
        assertNull(TimestampComparisonOperators.equal(nullLongBlock(), 0, longBlock(1_000_000L, 0), 0, 9L));
    }

    // --- LESS_THAN p=9 ---

    @Test
    public void testLessThanP9SameMicrosPicosTiebreaker()
    {
        long epochMicros = 1_000_000L;
        assertTrue(TimestampComparisonOperators.lessThan(longBlock(epochMicros, 0), 0, longBlock(epochMicros, 1), 0, 9L));
        assertFalse(TimestampComparisonOperators.lessThan(longBlock(epochMicros, 1), 0, longBlock(epochMicros, 0), 0, 9L));
    }

    // --- IS DISTINCT FROM ---

    @Test
    public void testDistinctFromNullVsNonNull()
    {
        assertFunction(
                "NULL IS DISTINCT FROM CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6))",
                BOOLEAN,
                true);
    }

    @Test
    public void testDistinctFromNullVsNull()
    {
        assertFunction(
                "CAST(NULL AS TIMESTAMP(6)) IS DISTINCT FROM CAST(NULL AS TIMESTAMP(6))",
                BOOLEAN,
                false);
    }

    @Test
    public void testDistinctFromP9NullVsNonNull()
    {
        assertTrue(TimestampComparisonOperators.isDistinctFrom(nullLongBlock(), 0, longBlock(1_000_000L, 0), 0, 9L));
    }

    @Test
    public void testDistinctFromP9NullVsNull()
    {
        assertFalse(TimestampComparisonOperators.isDistinctFrom(nullLongBlock(), 0, nullLongBlock(), 0, 9L));
    }

    // --- HASH_CODE p=9 ---

    @Test
    public void testHashCodeP9ConsistentWithEquality()
    {
        long epochMicros = 1_000_000L;
        Block b1 = longBlock(epochMicros, 500);
        Block b2 = longBlock(epochMicros, 500);
        assertEquals(
                TimestampComparisonOperators.hashCodeOperator(b1, 0, 9L),
                TimestampComparisonOperators.hashCodeOperator(b2, 0, 9L));
    }

    // --- p=3 regression: annotation-based operator in TimestampOperators still wins ---

    @Test
    public void testEqualP3RegressionExistingOverloadWins()
    {
        assertFunction(
                "TIMESTAMP '2024-01-01 00:00:00.000' = TIMESTAMP '2024-01-01 00:00:00.000'",
                BOOLEAN,
                true);
        assertFunction(
                "TIMESTAMP '2024-01-01 00:00:00.000' = TIMESTAMP '2024-01-01 00:00:00.001'",
                BOOLEAN,
                false);
    }
}

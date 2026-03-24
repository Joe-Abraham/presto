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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.type.TimestampParametricType.MAX_PRECISION;
import static com.facebook.presto.type.TimestampParametricType.MICROSECONDS_PRECISION_BOUNDARY;
import static com.facebook.presto.type.TimestampParametricType.TIMESTAMP_PARAMETRIC_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestTimestampParametricType
{
    @Test
    public void testName()
    {
        assertEquals(TIMESTAMP_PARAMETRIC_TYPE.getName(), "timestamp");
    }

    @Test
    public void testMaxPrecision()
    {
        assertEquals(MAX_PRECISION, 12);
    }

    @Test
    public void testMicrosecondsBoundary()
    {
        assertEquals(MICROSECONDS_PRECISION_BOUNDARY, 3);
    }

    @Test
    public void testDefaultPrecision()
    {
        // No parameters → default precision (3, milliseconds).
        Type type = TIMESTAMP_PARAMETRIC_TYPE.createType(ImmutableList.of());
        assertSame(type, TIMESTAMP);
    }

    @Test
    public void testMillisecondPrecisions()
    {
        // Precision 0, 1, 2, 3 should all resolve to TIMESTAMP (millisecond storage).
        for (int p = 0; p <= 3; p++) {
            List<TypeParameter> params = ImmutableList.of(longParameter(p));
            Type type = TIMESTAMP_PARAMETRIC_TYPE.createType(params);
            assertSame(type, TIMESTAMP, "Expected TIMESTAMP for precision " + p);
        }
    }

    @Test
    public void testMicrosecondPrecisions()
    {
        // Precision 4–12 should resolve to TIMESTAMP_MICROSECONDS (microsecond storage).
        for (int p = 4; p <= 12; p++) {
            List<TypeParameter> params = ImmutableList.of(longParameter(p));
            Type type = TIMESTAMP_PARAMETRIC_TYPE.createType(params);
            assertSame(type, TIMESTAMP_MICROSECONDS, "Expected TIMESTAMP_MICROSECONDS for precision " + p);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativePrecisionFails()
    {
        TIMESTAMP_PARAMETRIC_TYPE.createType(ImmutableList.of(longParameter(-1)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTooHighPrecisionFails()
    {
        TIMESTAMP_PARAMETRIC_TYPE.createType(ImmutableList.of(longParameter(13)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTooManyParametersFails()
    {
        TIMESTAMP_PARAMETRIC_TYPE.createType(ImmutableList.of(longParameter(3), longParameter(6)));
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private static TypeParameter longParameter(long value)
    {
        return TypeParameter.of(value);
    }
}

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

import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

public class TestTimestampWithTimeZoneType
{
    @Test
    public void testInterningReturnsSameReference()
    {
        for (int p = 0; p <= TimestampWithTimeZoneType.MAX_PRECISION; p++) {
            assertSame(createTimestampWithTimeZoneType(p), createTimestampWithTimeZoneType(p),
                    "createTimestampWithTimeZoneType(" + p + ") must return the same reference on repeated calls");
        }
    }

    @Test
    public void testConstantIsNonNull()
    {
        assertNotNull(TIMESTAMP_WITH_TIME_ZONE, "TIMESTAMP_WITH_TIME_ZONE constant must not be null");
    }

    @Test
    public void testConstantIsInterned()
    {
        assertSame(createTimestampWithTimeZoneType(3), TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testGetPrecision()
    {
        for (int p = 0; p <= TimestampWithTimeZoneType.MAX_PRECISION; p++) {
            assertEquals(createTimestampWithTimeZoneType(p).getPrecision(), p);
        }
    }

    @Test
    public void testInvalidPrecision()
    {
        expectThrows(IllegalArgumentException.class, () -> createTimestampWithTimeZoneType(-1));
        expectThrows(IllegalArgumentException.class, () -> createTimestampWithTimeZoneType(13));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> createTimestampWithTimeZoneType(13));
        assertEquals(e.getMessage().contains("13"), true);
    }

    @Test
    public void testReferenceEqualityIsTypeEquality()
    {
        for (int p1 = 0; p1 <= TimestampWithTimeZoneType.MAX_PRECISION; p1++) {
            for (int p2 = 0; p2 <= TimestampWithTimeZoneType.MAX_PRECISION; p2++) {
                if (p1 == p2) {
                    assertEquals(createTimestampWithTimeZoneType(p1), createTimestampWithTimeZoneType(p2));
                }
                else {
                    assertEquals(createTimestampWithTimeZoneType(p1).equals(createTimestampWithTimeZoneType(p2)), false,
                            "Types with different precision must not be equal");
                }
            }
        }
    }

    @Test
    public void testTypeSignatureForDefaultPrecision()
    {
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(TIMESTAMP_WITH_TIME_ZONE.getTypeSignature().getParameters().size(), 0);
    }

    @Test
    public void testTypeSignatureForNonDefaultPrecision()
    {
        TypeSignature sig = createTimestampWithTimeZoneType(9).getTypeSignature();
        assertEquals(sig.getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(sig.getParameters().size(), 1);
        assertEquals(sig.getParameters().get(0).getLongLiteral(), Long.valueOf(9));
    }

    @Test
    public void testTimestampParametricTypeRoundTrip()
    {
        TimestampParametricType parametric = new TimestampParametricType();
        for (int p = 0; p <= TimestampType.MAX_PRECISION; p++) {
            Type result = parametric.createType(List.of(TypeParameter.of((long) p)));
            assertSame(result, TimestampType.createTimestampType(p));
        }
    }

    @Test
    public void testTimestampWithTimeZoneParametricTypeRoundTrip()
    {
        TimestampWithTimeZoneParametricType parametric = new TimestampWithTimeZoneParametricType();
        for (int p = 0; p <= TimestampWithTimeZoneType.MAX_PRECISION; p++) {
            Type result = parametric.createType(List.of(TypeParameter.of((long) p)));
            assertSame(result, createTimestampWithTimeZoneType(p));
        }
    }

    @Test
    public void testParametricTypeNames()
    {
        assertEquals(new TimestampParametricType().getName(), StandardTypes.TIMESTAMP);
        assertEquals(new TimestampWithTimeZoneParametricType().getName(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
    }
}

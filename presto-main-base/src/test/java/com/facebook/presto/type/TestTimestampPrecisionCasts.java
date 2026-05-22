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

import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestTimestampPrecisionCasts
        extends AbstractTestFunctions
{
    public TestTimestampPrecisionCasts()
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .build());
    }

    @Test
    public void testWideningCast()
    {
        // CAST(TIMESTAMP(3) AS TIMESTAMP(6)) zero-fills sub-millisecond digits
        SqlTimestamp ts3 = sqlTimestampOf(2001, 8, 22, 3, 4, 5, 321, session);
        long micros = ts3.getMicros();
        assertFunction(
                "CAST(TIMESTAMP '2001-08-22 03:04:05.321' AS TIMESTAMP(6))",
                TIMESTAMP_MICROSECONDS,
                sqlTimestampOf(micros, session.toConnectorSession(), TimeUnit.MICROSECONDS));
    }

    @Test
    public void testNarrowingCast()
    {
        // CAST(TIMESTAMP(6) AS TIMESTAMP(3)) with exact millisecond value (no truncation needed)
        SqlTimestamp ts3 = sqlTimestampOf(2001, 8, 22, 3, 4, 5, 321, session);
        assertFunction(
                "CAST(CAST(TIMESTAMP '2001-08-22 03:04:05.321' AS TIMESTAMP(6)) AS TIMESTAMP(3))",
                TIMESTAMP,
                ts3);
    }

    @Test
    public void testFloorTruncationOnPositive()
    {
        // CAST(TIMESTAMP(6) AS TIMESTAMP(3)): 999 ms widens to 999_000 μs, truncates back to 999 ms
        SqlTimestamp ts3 = sqlTimestampOf(1970, 1, 1, 0, 0, 0, 999, session);
        assertFunction(
                "CAST(CAST(TIMESTAMP '1970-01-01 00:00:00.999' AS TIMESTAMP(6)) AS TIMESTAMP(3))",
                TIMESTAMP,
                ts3);
    }

    @Test
    public void testFloorTruncationOnNegative()
    {
        // CAST(TIMESTAMP(6) AS TIMESTAMP(3)): -1 ms → -1000 μs → floor(-1000/1000) = -1 ms
        SqlTimestamp ts3 = sqlTimestampOf(1969, 12, 31, 23, 59, 59, 999, session);
        assertFunction(
                "CAST(CAST(TIMESTAMP '1969-12-31 23:59:59.999' AS TIMESTAMP(6)) AS TIMESTAMP(3))",
                TIMESTAMP,
                ts3);
    }

    @Test
    public void testWideningThenNarrowingRoundTrip()
    {
        // Round-trip through higher precision should give back the original value
        SqlTimestamp ts3 = sqlTimestampOf(2001, 8, 22, 3, 4, 5, 321, session);
        assertFunction(
                "CAST(CAST(CAST(TIMESTAMP '2001-08-22 03:04:05.321' AS TIMESTAMP(6)) AS TIMESTAMP(3)) AS TIMESTAMP(6))",
                TIMESTAMP_MICROSECONDS,
                sqlTimestampOf(ts3.getMicros(), session.toConnectorSession(), TimeUnit.MICROSECONDS));
    }

    @Test
    public void testShortToShortCastArithmetic()
    {
        // Direct unit test of shortToShortCast arithmetic
        assertEquals(TimestampPrecisionCasts.shortToShortCast(1000L, 3, 6), 1_000_000L);  // ms→μs: *1000
        assertEquals(TimestampPrecisionCasts.shortToShortCast(1_000_000L, 6, 3), 1000L);  // μs→ms: /1000
        assertEquals(TimestampPrecisionCasts.shortToShortCast(-1L, 3, 6), -1000L);         // negative ms→μs
        assertEquals(TimestampPrecisionCasts.shortToShortCast(-999L, 6, 3), -1L);          // floor(-999/1000)=-1
        assertEquals(TimestampPrecisionCasts.shortToShortCast(-1000L, 6, 3), -1L);         // exact boundary
        assertEquals(TimestampPrecisionCasts.shortToShortCast(-1001L, 6, 3), -2L);         // floor(-1001/1000)=-2
        assertEquals(TimestampPrecisionCasts.shortToShortCast(0L, 3, 6), 0L);              // epoch zero
        assertEquals(TimestampPrecisionCasts.shortToShortCast(999L, 3, 0), 0L);            // ms→s floor(0.999)=0
        assertEquals(TimestampPrecisionCasts.shortToShortCast(-1L, 3, 0), -1L);            // floor(-0.001)=-1
    }
}

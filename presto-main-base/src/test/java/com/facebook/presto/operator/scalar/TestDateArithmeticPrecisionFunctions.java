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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.type.SqlTimestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestDateArithmeticPrecisionFunctions
        extends AbstractTestFunctions
{
    public TestDateArithmeticPrecisionFunctions()
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .build());
    }

    // --- date_add: precision preservation ---

    @Test
    public void testDateAddHourPreservesP6()
    {
        long expectedMicros = new DateTime(2024, 1, 1, 2, 30, 0, 321, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "date_add('hour', 1, CAST(TIMESTAMP '2024-01-01 01:30:00.321' AS TIMESTAMP(6)))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testDateAddP3Unchanged()
    {
        // Existing annotation-based overload still wins for p=3 input (regression check)
        assertFunction(
                "date_add('hour', 1, TIMESTAMP '2024-01-01 01:30:00.321')",
                TIMESTAMP,
                sqlTimestampOf(2024, 1, 1, 2, 30, 0, 321, session));
    }

    @Test
    public void testDateAddSubMillisPreserved()
    {
        // Sub-millisecond component must survive adding 1 day (all units are ≥ 1ms)
        long epochMillis = new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
        long epochMicros = epochMillis * 1000L + 456L; // 2024-01-01 00:00:00.000456 UTC
        long oneDayMillis = 86_400_000L;

        long result = DateArithmeticPrecisionFunctions.addIntervalDayToSecond(epochMicros, oneDayMillis, 6);

        long expectedMicros = (epochMillis + oneDayMillis) * 1000L + 456L;
        assertEquals(result, expectedMicros);
    }

    // --- date_diff: precision preservation ---

    @Test
    public void testDateDiffP6()
    {
        assertFunction(
                "date_diff('second',"
                        + " CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6)),"
                        + " CAST(TIMESTAMP '2024-01-01 01:00:00.000' AS TIMESTAMP(6)))",
                BIGINT,
                3600L);
    }

    @Test
    public void testDateDiffMixedPrecision()
    {
        // TIMESTAMP(3) arg is widened to TIMESTAMP(6) via implicit coercion (Story 2.2)
        assertFunction(
                "date_diff('second',"
                        + " TIMESTAMP '2024-01-01 00:00:00.000',"
                        + " CAST(TIMESTAMP '2024-01-01 01:00:00.000' AS TIMESTAMP(6)))",
                BIGINT,
                3600L);
    }

    // --- INTERVAL arithmetic: precision preservation ---

    @Test
    public void testTimestampPlusIntervalDayPreservesP6()
    {
        long expectedMicros = new DateTime(2024, 1, 2, 0, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6)) + INTERVAL '1' DAY",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testIntervalDayPlusTimestampPreservesP6()
    {
        // Commutative form: INTERVAL + TIMESTAMP
        long expectedMicros = new DateTime(2024, 1, 2, 0, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "INTERVAL '1' DAY + CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testTimestampMinusIntervalDayPreservesP6()
    {
        long expectedMicros = new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "CAST(TIMESTAMP '2024-01-02 00:00:00.000' AS TIMESTAMP(6)) - INTERVAL '1' DAY",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testTimestampPlusIntervalYearPreservesP6()
    {
        long expectedMicros = new DateTime(2025, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "CAST(TIMESTAMP '2024-01-01 00:00:00.000' AS TIMESTAMP(6)) + INTERVAL '1' YEAR",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }
}

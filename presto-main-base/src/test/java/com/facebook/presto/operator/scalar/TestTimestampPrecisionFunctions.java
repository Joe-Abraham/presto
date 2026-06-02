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
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimeZoneKey;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestTimestampPrecisionFunctions
        extends AbstractTestFunctions
{
    public TestTimestampPrecisionFunctions()
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "false")
                .build());
    }

    // --- date_trunc: precision preservation ---

    @Test
    public void testDateTruncHourPreservesP6()
    {
        // TIMESTAMP(6) input must produce TIMESTAMP(6) output, not TIMESTAMP(3)
        long expectedMicros = new DateTime(2024, 1, 1, 1, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "date_trunc('hour', CAST(TIMESTAMP '2024-01-01 01:30:45.321' AS TIMESTAMP(6)))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testDateTruncMinutePreservesP6()
    {
        long expectedMicros = new DateTime(2024, 1, 1, 1, 30, 0, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "date_trunc('minute', CAST(TIMESTAMP '2024-01-01 01:30:45.321' AS TIMESTAMP(6)))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testDateTruncSecondPreservesP6()
    {
        // Sub-second digits are zeroed; type stays TIMESTAMP(6)
        long expectedMicros = new DateTime(2024, 1, 1, 1, 30, 45, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "date_trunc('second', CAST(TIMESTAMP '2024-01-01 01:30:45.321' AS TIMESTAMP(6)))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testDateTruncNegativeEpochP6()
    {
        // floorDiv required for pre-1970 timestamps: 1969-12-31 23:59:59.999 truncated to day
        long expectedMicros = new DateTime(1969, 12, 31, 0, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000L;
        assertFunction(
                "date_trunc('day', CAST(TIMESTAMP '1969-12-31 23:59:59.999' AS TIMESTAMP(6)))",
                TIMESTAMP_MICROSECONDS,
                new SqlTimestamp(expectedMicros, TimeUnit.MICROSECONDS));
    }

    @Test
    public void testDateTruncP3Unchanged()
    {
        // Existing p=3 behavior is not affected: exact-type annotation overload still wins
        assertFunction(
                "date_trunc('minute', TIMESTAMP '2024-01-01 01:30:45.321')",
                TIMESTAMP,
                sqlTimestampOf(2024, 1, 1, 1, 30, 0, 0, session));
    }

    // --- at_timezone: precision preservation ---

    @Test
    public void testAtTimezoneP3Unchanged()
    {
        // Existing p=3 TIMESTAMP_WITH_TIME_ZONE: exact-type annotation overload still wins (regression)
        long millis = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
        TimeZoneKey nyKey = getTimeZoneKey("America/New_York");
        assertFunction(
                "at_timezone(from_unixtime(0, 'UTC'), 'America/New_York')",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(millis, nyKey));
    }

    @Test
    public void testAtTimezoneImplPreservesMillisAndChangesZone()
    {
        // Direct unit test of the implementation: millis are preserved, only zone changes
        long millis = new DateTime(2024, 6, 15, 12, 0, 0, 0, DateTimeZone.UTC).getMillis();
        long packed = packDateTimeWithZone(millis, UTC_KEY);

        long result = TimestampPrecisionFunctions.atTimezone(packed, Slices.utf8Slice("America/New_York"));

        TimeZoneKey nyKey = getTimeZoneKey("America/New_York");
        assertEquals(unpackMillisUtc(result), millis);
        assertEquals(unpackZoneKey(result), nyKey);
    }
}

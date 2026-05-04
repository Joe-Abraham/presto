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

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;

/**
 * Represents a TIMESTAMP WITH TIME ZONE value with sub-millisecond precision (precision 4-12).
 * <p>
 * Storage encoding (matching Fixed12Block layout):
 * <ul>
 *   <li>First 8 bytes: epoch milliseconds (UTC) as a {@code long}</li>
 *   <li>Last 4 bytes: {@code int} packed as {@code (picosOfMilli << 12) | (timeZoneKey & 0xFFF)}</li>
 * </ul>
 * <p>
 * {@code picosOfMilli} stores picoseconds within the millisecond (0 to 999,999,999).
 * With 20 bits available (after the 12-bit timezone key), values up to 1,048,575 are stored
 * directly; this accommodates nanosecond-within-millisecond precision (0 to 999,999).
 */
public final class LongTimestampWithTimeZone
        implements Comparable<LongTimestampWithTimeZone>
{
    // JSON format matches SqlTimestampWithTimeZone for display consistency
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS VV");

    private final long epochMillis;
    private final int picosOfMilli;
    private final short timeZoneKey;

    public LongTimestampWithTimeZone(long epochMillis, int picosOfMilli, short timeZoneKey)
    {
        this.epochMillis = epochMillis;
        this.picosOfMilli = picosOfMilli;
        this.timeZoneKey = timeZoneKey;
    }

    public long getEpochMillis()
    {
        return epochMillis;
    }

    public int getPicosOfMilli()
    {
        return picosOfMilli;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return TimeZoneKey.getTimeZoneKey(timeZoneKey);
    }

    /**
     * Packs picosOfMilli and timeZoneKey into a single int for Fixed12Block storage.
     * Layout: bits 31:12 = picosOfMilli, bits 11:0 = timeZoneKey.
     */
    public static int packFraction(int picosOfMilli, short timeZoneKey)
    {
        return (picosOfMilli << 12) | (timeZoneKey & 0xFFF);
    }

    /**
     * Extracts picosOfMilli from a packed fraction int (bits 31:12).
     */
    public static int unpackPicosOfMilli(int packed)
    {
        return (packed >>> 12);
    }

    /**
     * Extracts timeZoneKey from a packed fraction int (bits 11:0).
     */
    public static short unpackTimeZoneKey(int packed)
    {
        return (short) (packed & 0xFFF);
    }

    @Override
    public int compareTo(LongTimestampWithTimeZone other)
    {
        int result = Long.compare(epochMillis, other.epochMillis);
        if (result != 0) {
            return result;
        }
        return Integer.compare(picosOfMilli, other.picosOfMilli);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongTimestampWithTimeZone that = (LongTimestampWithTimeZone) o;
        return epochMillis == that.epochMillis &&
                picosOfMilli == that.picosOfMilli &&
                timeZoneKey == that.timeZoneKey;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epochMillis, picosOfMilli, timeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        long epochSecond = floorDiv(epochMillis, 1_000L);
        int millisOfSecond = toIntExact(floorMod(epochMillis, 1_000L));

        // Convert millis + picosOfMilli to nanoseconds for Instant construction
        long nanosOfSecond = millisOfSecond * 1_000_000L + (picosOfMilli / 1_000L);
        Instant instant = Instant.ofEpochSecond(epochSecond, nanosOfSecond);

        TimeZoneKey tzKey = TimeZoneKey.getTimeZoneKey(timeZoneKey);
        ZoneId zone = ZoneId.of(tzKey.getId());
        return instant.atZone(zone).format(FORMATTER);
    }
}

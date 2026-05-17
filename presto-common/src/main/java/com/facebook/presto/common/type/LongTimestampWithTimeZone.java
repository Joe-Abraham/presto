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
 *   <li>Last 4 bytes: {@code int} packed as {@code (nanosOfMilli << 12) | (timeZoneKey & 0xFFF)}</li>
 * </ul>
 * <p>
 * {@code nanosOfMilli} stores nanoseconds within the millisecond (0 to 999,999).
 * The packed {@code int} allocates 20 bits (after the 12-bit timezone key), which is sufficient
 * for values up to 1,048,575 — enough to represent nanosecond-within-millisecond precision.
 */
public final class LongTimestampWithTimeZone
        implements Comparable<LongTimestampWithTimeZone>
{
    // JSON format matches SqlTimestampWithTimeZone for display consistency
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS VV");

    private final long epochMillis;
    private final int nanosOfMilli;
    private final short timeZoneKey;

    public LongTimestampWithTimeZone(long epochMillis, int nanosOfMilli, short timeZoneKey)
    {
        this.epochMillis = epochMillis;
        this.nanosOfMilli = nanosOfMilli;
        this.timeZoneKey = timeZoneKey;
    }

    public long getEpochMillis()
    {
        return epochMillis;
    }

    public int getNanosOfMilli()
    {
        return nanosOfMilli;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return TimeZoneKey.getTimeZoneKey(timeZoneKey);
    }

    /**
     * Packs nanosOfMilli and timeZoneKey into a single int for Fixed12Block storage.
     * Layout: bits 31:12 = nanosOfMilli, bits 11:0 = timeZoneKey.
     */
    public static int packFraction(int nanosOfMilli, short timeZoneKey)
    {
        return (nanosOfMilli << 12) | (timeZoneKey & 0xFFF);
    }

    /**
     * Extracts nanosOfMilli from a packed fraction int (bits 31:12).
     */
    public static int unpackNanosOfMilli(int packed)
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
        return Integer.compare(nanosOfMilli, other.nanosOfMilli);
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
                nanosOfMilli == that.nanosOfMilli &&
                timeZoneKey == that.timeZoneKey;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epochMillis, nanosOfMilli, timeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        long epochSecond = floorDiv(epochMillis, 1_000L);
        int millisOfSecond = toIntExact(floorMod(epochMillis, 1_000L));

        long nanosOfSecond = millisOfSecond * 1_000_000L + nanosOfMilli;
        Instant instant = Instant.ofEpochSecond(epochSecond, nanosOfSecond);

        TimeZoneKey tzKey = TimeZoneKey.getTimeZoneKey(timeZoneKey);
        ZoneId zone = ZoneId.of(tzKey.getId());
        return instant.atZone(zone).format(FORMATTER);
    }
}

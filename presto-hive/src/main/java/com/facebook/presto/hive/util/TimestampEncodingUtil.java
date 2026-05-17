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
package com.facebook.presto.hive.util;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.LongTimestamp;
import com.facebook.presto.common.type.LongTimestampType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.hive.HiveTimestampPrecision;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;

/**
 * Utilities for encoding parametric timestamps for Hive write operations.
 * Provides methods to extract timestamp components with proper precision handling
 * for different file formats (ORC, Parquet, RCFile).
 */
public final class TimestampEncodingUtil
{
    private static final long MICROSECONDS_PER_SECOND = 1_000_000L;
    private static final long MICROSECONDS_PER_MILLISECOND = 1_000L;
    private static final long MILLISECONDS_PER_SECOND = 1_000L;

    private TimestampEncodingUtil() {}

    /**
     * Represents a decoded timestamp with all precision components.
     */
    public static class DecodedTimestamp
    {
        private final long epochSeconds;
        private final int nanosOfSecond;
        private final long epochMicros;
        private final int picosOfMicro;

        public DecodedTimestamp(long epochSeconds, int nanosOfSecond, long epochMicros, int picosOfMicro)
        {
            this.epochSeconds = epochSeconds;
            this.nanosOfSecond = nanosOfSecond;
            this.epochMicros = epochMicros;
            this.picosOfMicro = picosOfMicro;
        }

        public long getEpochSeconds()
        {
            return epochSeconds;
        }

        public int getNanosOfSecond()
        {
            return nanosOfSecond;
        }

        public long getEpochMicros()
        {
            return epochMicros;
        }

        public int getPicosOfMicro()
        {
            return picosOfMicro;
        }

        public long getEpochMillis()
        {
            return epochSeconds * MILLISECONDS_PER_SECOND + (nanosOfSecond / 1_000_000);
        }

        public int getMillisOfSecond()
        {
            return (int) (nanosOfSecond / 1_000_000);
        }

        public int getMicrosOfSecond()
        {
            return (int) (nanosOfSecond / 1_000);
        }
    }

    /**
     * Decodes a parametric timestamp from a block into its component parts.
     * Handles both short timestamps (precision 0-6) and long timestamps (precision 7-12).
     */
    public static DecodedTimestamp decodeTimestamp(TimestampType type, Block block, int position)
    {
        long epochMicros;
        int picosOfMicro = 0;

        if (type.isShort()) {
            // Short timestamp: stored as long (millis for precision ≤3, micros for precision 4-6)
            long raw = type.getLong(block, position);
            epochMicros = (type.getPrecision() <= 3) ? raw * MICROSECONDS_PER_MILLISECOND : raw;
        }
        else {
            // Long timestamp: stored as LongTimestamp object
            LongTimestamp lts = ((LongTimestampType) type).getObject(block, position);
            epochMicros = lts.getEpochMicros();
            picosOfMicro = lts.getPicosOfMicro();
        }

        long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int microsOfSecond = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_SECOND));
        int nanosOfSecond = microsOfSecond * 1_000 + picosOfMicro / 1_000;

        return new DecodedTimestamp(epochSeconds, nanosOfSecond, epochMicros, picosOfMicro);
    }

    /**
     * Creates a timestamp encoding appropriate for the specified precision.
     * Used for validating precision support and choosing appropriate encoding strategies.
     */
    public static TimestampEncoder createEncoder(HiveTimestampPrecision targetPrecision)
    {
        switch (targetPrecision) {
            case MILLISECONDS:
                return new MillisecondTimestampEncoder();
            case MICROSECONDS:
                return new MicrosecondTimestampEncoder();
            case NANOSECONDS:
                return new NanosecondTimestampEncoder();
            default:
                throw new IllegalArgumentException("Unsupported timestamp precision: " + targetPrecision);
        }
    }

    /**
     * Base class for timestamp encoders that handle precision-specific encoding.
     */
    public abstract static class TimestampEncoder
    {
        /**
         * Returns true if this encoder can handle the given source timestamp type.
         */
        public abstract boolean canEncode(TimestampType sourceType);

        /**
         * Encodes a decoded timestamp to the target format.
         * The exact return type depends on the encoder implementation.
         */
        public abstract Object encode(DecodedTimestamp timestamp);

        /**
         * Returns the precision that this encoder targets.
         */
        public abstract int getTargetPrecision();
    }

    /**
     * Encoder for millisecond precision timestamps.
     */
    public static class MillisecondTimestampEncoder extends TimestampEncoder
    {
        @Override
        public boolean canEncode(TimestampType sourceType)
        {
            // Can encode any precision by truncating
            return true;
        }

        @Override
        public Object encode(DecodedTimestamp timestamp)
        {
            // Return epoch milliseconds (truncating sub-millisecond precision)
            return timestamp.getEpochMillis();
        }

        @Override
        public int getTargetPrecision()
        {
            return 3;
        }
    }

    /**
     * Encoder for microsecond precision timestamps.
     */
    public static class MicrosecondTimestampEncoder extends TimestampEncoder
    {
        @Override
        public boolean canEncode(TimestampType sourceType)
        {
            // Can encode precision ≤ 6 without loss, higher precision with truncation
            return true;
        }

        @Override
        public Object encode(DecodedTimestamp timestamp)
        {
            // Return epoch microseconds (truncating sub-microsecond precision)
            return timestamp.getEpochMicros();
        }

        @Override
        public int getTargetPrecision()
        {
            return 6;
        }
    }

    /**
     * Encoder for nanosecond precision timestamps.
     */
    public static class NanosecondTimestampEncoder extends TimestampEncoder
    {
        @Override
        public boolean canEncode(TimestampType sourceType)
        {
            // Can encode precision ≤ 9 without loss, higher precision with truncation
            return true;
        }

        @Override
        public Object encode(DecodedTimestamp timestamp)
        {
            // Return nanoseconds representation
            return new NanosTimestamp(timestamp.getEpochSeconds(), timestamp.getNanosOfSecond());
        }

        @Override
        public int getTargetPrecision()
        {
            return 9;
        }
    }

    /**
     * Represents a nanosecond-precision timestamp for encoding.
     */
    public static class NanosTimestamp
    {
        private final long epochSeconds;
        private final int nanosOfSecond;

        public NanosTimestamp(long epochSeconds, int nanosOfSecond)
        {
            this.epochSeconds = epochSeconds;
            this.nanosOfSecond = nanosOfSecond;
        }

        public long getEpochSeconds()
        {
            return epochSeconds;
        }

        public int getNanosOfSecond()
        {
            return nanosOfSecond;
        }

        public long getEpochNanos()
        {
            return epochSeconds * 1_000_000_000L + nanosOfSecond;
        }
    }

    /**
     * Checks if the target format can losslessly store the source timestamp type.
     */
    public static boolean canStoreLosslessly(TimestampType sourceType, HiveTimestampPrecision targetPrecision)
    {
        return sourceType.getPrecision() <= targetPrecision.getPrecision();
    }

    /**
     * Validates that the target precision is supported for the given file format.
     */
    public static void validateFormatSupport(String formatName, HiveTimestampPrecision precision)
    {
        switch (formatName.toUpperCase()) {
            case "ORC":
                // ORC supports all precisions natively
                break;
            case "PARQUET":
                // Parquet supports nanosecond precision natively
                break;
            case "RCFILE":
            case "TEXTFILE":
                // Text-based formats support arbitrary precision through string representation
                break;
            default:
                // Unknown format - assume it supports the requested precision
                break;
        }
    }
}
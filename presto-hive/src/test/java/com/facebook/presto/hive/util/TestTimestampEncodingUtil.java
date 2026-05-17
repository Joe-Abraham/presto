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
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.LongTimestamp;
import com.facebook.presto.common.type.LongTimestampType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.hive.HiveTimestampPrecision;
import com.facebook.presto.hive.util.TimestampEncodingUtil.DecodedTimestamp;
import com.facebook.presto.hive.util.TimestampEncodingUtil.MillisecondTimestampEncoder;
import com.facebook.presto.hive.util.TimestampEncodingUtil.MicrosecondTimestampEncoder;
import com.facebook.presto.hive.util.TimestampEncodingUtil.NanosecondTimestampEncoder;
import com.facebook.presto.hive.util.TimestampEncodingUtil.NanosTimestamp;
import com.facebook.presto.hive.util.TimestampEncodingUtil.TimestampEncoder;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTimestampEncodingUtil
{
    @Test
    public void testDecodeShortTimestamp()
    {
        // Test decoding short timestamp (precision 3 - milliseconds)
        TimestampType type3 = createTimestampType(3);
        long epochMillis = 1672574400123L; // 2023-01-01 12:00:00.123 UTC
        
        Block block = new LongArrayBlockBuilder(null, 1)
                .writeLong(epochMillis)
                .build();
        
        DecodedTimestamp decoded = TimestampEncodingUtil.decodeTimestamp(type3, block, 0);
        
        assertEquals(decoded.getEpochSeconds(), 1672574400L);
        assertEquals(decoded.getNanosOfSecond(), 123_000_000);
        assertEquals(decoded.getEpochMicros(), epochMillis * 1000L);
        assertEquals(decoded.getPicosOfMicro(), 0);
    }

    @Test
    public void testDecodeShortTimestampMicroseconds()
    {
        // Test decoding short timestamp (precision 6 - microseconds)
        TimestampType type6 = createTimestampType(6);
        long epochMicros = 1672574400123456L; // 2023-01-01 12:00:00.123456 UTC
        
        Block block = new LongArrayBlockBuilder(null, 1)
                .writeLong(epochMicros)
                .build();
        
        DecodedTimestamp decoded = TimestampEncodingUtil.decodeTimestamp(type6, block, 0);
        
        assertEquals(decoded.getEpochSeconds(), 1672574400L);
        assertEquals(decoded.getNanosOfSecond(), 123_456_000);
        assertEquals(decoded.getEpochMicros(), epochMicros);
        assertEquals(decoded.getPicosOfMicro(), 0);
    }

    @Test
    public void testMillisecondEncoder()
    {
        MillisecondTimestampEncoder encoder = new MillisecondTimestampEncoder();
        
        // Test that it can encode any precision
        assertTrue(encoder.canEncode(createTimestampType(0)));
        assertTrue(encoder.canEncode(createTimestampType(6)));
        assertTrue(encoder.canEncode(createTimestampType(9)));
        
        assertEquals(encoder.getTargetPrecision(), 3);
        
        // Test encoding
        DecodedTimestamp timestamp = new DecodedTimestamp(
                1672574400L, 123_456_789, 1672574400123456L, 789);
        
        Object encoded = encoder.encode(timestamp);
        assertEquals(encoded, 1672574400123L); // Truncated to milliseconds
    }

    @Test
    public void testMicrosecondEncoder()
    {
        MicrosecondTimestampEncoder encoder = new MicrosecondTimestampEncoder();
        
        assertTrue(encoder.canEncode(createTimestampType(6)));
        assertTrue(encoder.canEncode(createTimestampType(9))); // With truncation
        
        assertEquals(encoder.getTargetPrecision(), 6);
        
        // Test encoding
        DecodedTimestamp timestamp = new DecodedTimestamp(
                1672574400L, 123_456_789, 1672574400123456L, 789);
        
        Object encoded = encoder.encode(timestamp);
        assertEquals(encoded, 1672574400123456L); // Truncated to microseconds
    }

    @Test
    public void testNanosecondEncoder()
    {
        NanosecondTimestampEncoder encoder = new NanosecondTimestampEncoder();
        
        assertTrue(encoder.canEncode(createTimestampType(9)));
        assertTrue(encoder.canEncode(createTimestampType(12))); // With truncation
        
        assertEquals(encoder.getTargetPrecision(), 9);
        
        // Test encoding
        DecodedTimestamp timestamp = new DecodedTimestamp(
                1672574400L, 123_456_789, 1672574400123456L, 789);
        
        Object encoded = encoder.encode(timestamp);
        assertTrue(encoded instanceof NanosTimestamp);
        
        NanosTimestamp nanosTimestamp = (NanosTimestamp) encoded;
        assertEquals(nanosTimestamp.getEpochSeconds(), 1672574400L);
        assertEquals(nanosTimestamp.getNanosOfSecond(), 123_456_789);
    }

    @Test
    public void testCreateEncoder()
    {
        TimestampEncoder encoder1 = TimestampEncodingUtil.createEncoder(HiveTimestampPrecision.MILLISECONDS);
        assertTrue(encoder1 instanceof MillisecondTimestampEncoder);
        
        TimestampEncoder encoder2 = TimestampEncodingUtil.createEncoder(HiveTimestampPrecision.MICROSECONDS);
        assertTrue(encoder2 instanceof MicrosecondTimestampEncoder);
        
        TimestampEncoder encoder3 = TimestampEncodingUtil.createEncoder(HiveTimestampPrecision.NANOSECONDS);
        assertTrue(encoder3 instanceof NanosecondTimestampEncoder);
    }

    @Test
    public void testCanStoreLosslessly()
    {
        // Test precision compatibility
        assertTrue(TimestampEncodingUtil.canStoreLosslessly(
                createTimestampType(3), HiveTimestampPrecision.MILLISECONDS));
        assertTrue(TimestampEncodingUtil.canStoreLosslessly(
                createTimestampType(6), HiveTimestampPrecision.MICROSECONDS));
        assertTrue(TimestampEncodingUtil.canStoreLosslessly(
                createTimestampType(9), HiveTimestampPrecision.NANOSECONDS));
        
        // Test precision loss cases
        assertFalse(TimestampEncodingUtil.canStoreLosslessly(
                createTimestampType(6), HiveTimestampPrecision.MILLISECONDS));
        assertFalse(TimestampEncodingUtil.canStoreLosslessly(
                createTimestampType(9), HiveTimestampPrecision.MICROSECONDS));
        
        // Test higher precision can store lower precision
        assertTrue(TimestampEncodingUtil.canStoreLosslessly(
                createTimestampType(3), HiveTimestampPrecision.MICROSECONDS));
        assertTrue(TimestampEncodingUtil.canStoreLosslessly(
                createTimestampType(6), HiveTimestampPrecision.NANOSECONDS));
    }

    @Test
    public void testValidateFormatSupport()
    {
        // These should not throw exceptions
        TimestampEncodingUtil.validateFormatSupport("ORC", HiveTimestampPrecision.NANOSECONDS);
        TimestampEncodingUtil.validateFormatSupport("PARQUET", HiveTimestampPrecision.NANOSECONDS);
        TimestampEncodingUtil.validateFormatSupport("RCFILE", HiveTimestampPrecision.NANOSECONDS);
        TimestampEncodingUtil.validateFormatSupport("TEXTFILE", HiveTimestampPrecision.NANOSECONDS);
        TimestampEncodingUtil.validateFormatSupport("UNKNOWN_FORMAT", HiveTimestampPrecision.MILLISECONDS);
    }

    @Test
    public void testDecodedTimestampMethods()
    {
        DecodedTimestamp timestamp = new DecodedTimestamp(
                1672574400L, 123_456_789, 1672574400123456L, 789);
        
        assertEquals(timestamp.getEpochSeconds(), 1672574400L);
        assertEquals(timestamp.getNanosOfSecond(), 123_456_789);
        assertEquals(timestamp.getEpochMicros(), 1672574400123456L);
        assertEquals(timestamp.getPicosOfMicro(), 789);
        
        assertEquals(timestamp.getEpochMillis(), 1672574400123L);
        assertEquals(timestamp.getMillisOfSecond(), 123);
        assertEquals(timestamp.getMicrosOfSecond(), 123_456);
    }

    @Test
    public void testNanosTimestamp()
    {
        NanosTimestamp nanosTimestamp = new NanosTimestamp(1672574400L, 123_456_789);
        
        assertEquals(nanosTimestamp.getEpochSeconds(), 1672574400L);
        assertEquals(nanosTimestamp.getNanosOfSecond(), 123_456_789);
        assertEquals(nanosTimestamp.getEpochNanos(), 1672574400123456789L);
    }

    @Test
    public void testPrecisionBoundaries()
    {
        // Test precision 0 (seconds)
        TimestampType type0 = createTimestampType(0);
        long epochSeconds = 1672574400L; // 2023-01-01 12:00:00 UTC
        
        Block block0 = new LongArrayBlockBuilder(null, 1)
                .writeLong(epochSeconds * 1000L) // Stored as millis
                .build();
        
        DecodedTimestamp decoded0 = TimestampEncodingUtil.decodeTimestamp(type0, block0, 0);
        assertEquals(decoded0.getEpochSeconds(), epochSeconds);
        assertEquals(decoded0.getNanosOfSecond(), 0);
        
        // Test precision 12 (maximum)
        TimestampType type12 = createTimestampType(12);
        
        // For precision 12, we would use LongTimestamp, but we'll test the concept
        DecodedTimestamp highPrecision = new DecodedTimestamp(
                epochSeconds, 123_456_789, epochSeconds * 1_000_000L + 123_456L, 789_000);
        
        assertEquals(highPrecision.getEpochSeconds(), epochSeconds);
        assertEquals(highPrecision.getNanosOfSecond(), 123_456_789);
        assertEquals(highPrecision.getPicosOfMicro(), 789_000);
    }

    @Test
    public void testEncoderCompatibility()
    {
        // Test that encoders handle different input precisions correctly
        DecodedTimestamp highPrecisionInput = new DecodedTimestamp(
                1672574400L, 123_456_789, 1672574400123456L, 789_000);
        
        // Millisecond encoder should truncate
        MillisecondTimestampEncoder millisEncoder = new MillisecondTimestampEncoder();
        assertEquals(millisEncoder.encode(highPrecisionInput), 1672574400123L);
        
        // Microsecond encoder should preserve microseconds, truncate nanos
        MicrosecondTimestampEncoder microsEncoder = new MicrosecondTimestampEncoder();
        assertEquals(microsEncoder.encode(highPrecisionInput), 1672574400123456L);
        
        // Nanosecond encoder should preserve nanoseconds
        NanosecondTimestampEncoder nanosEncoder = new NanosecondTimestampEncoder();
        NanosTimestamp nanosResult = (NanosTimestamp) nanosEncoder.encode(highPrecisionInput);
        assertEquals(nanosResult.getNanosOfSecond(), 123_456_789);
    }
}
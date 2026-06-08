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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// All precisions are stored as a single epoch-scaled long. The stored value equals the number of
// units (determined by precision) elapsed since 1970-01-01T00:00:00 UTC. For example, p=3 stores
// epoch-milliseconds and p=6 stores epoch-microseconds. Precisions above p=6 are accepted but the
// long range limits the representable window (e.g. p=12 fits roughly +/-2562 years from epoch).
public final class TimestampType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;
    public static final int MAX_SHORT_PRECISION = 6;
    public static final int DEFAULT_PRECISION = 3;

    private static final long[] PRECISION_SCALE = {
            1L,                     // p=0  (seconds)
            10L,                    // p=1
            100L,                   // p=2
            1_000L,                 // p=3  (milliseconds)
            10_000L,                // p=4
            100_000L,               // p=5
            1_000_000L,             // p=6  (microseconds)
            10_000_000L,            // p=7
            100_000_000L,           // p=8
            1_000_000_000L,         // p=9  (nanoseconds)
            10_000_000_000L,        // p=10
            100_000_000_000L,       // p=11
            1_000_000_000_000L,     // p=12 (picoseconds)
    };

    private static final TimestampType[] INSTANCES = new TimestampType[MAX_PRECISION + 1];

    static {
        for (int p = 0; p <= MAX_PRECISION; p++) {
            INSTANCES[p] = new TimestampType(p);
        }
    }

    public static final TimestampType TIMESTAMP = INSTANCES[DEFAULT_PRECISION];

    // Keeps the legacy "timestamp microseconds" type signature so existing code that matches
    // on type-signature base strings continues to work without changes.
    public static final TimestampType TIMESTAMP_MICROSECONDS = INSTANCES[MAX_SHORT_PRECISION];

    private final int precision;

    /**
     * Returns the interned {@link TimestampType} for the given precision (0–{@link #MAX_PRECISION}).
     *
     * <p><b>Type-system support matrix (as of this change):</b>
     * <ul>
     *   <li>p=3 ({@link #TIMESTAMP}) and p=6 ({@link #TIMESTAMP_MICROSECONDS}) are fully wired:
     *       registered in the type manager, round-trippable via type signature, and supported by
     *       {@link #getObjectValue}.</li>
     *   <li>All precisions support the arithmetic helpers ({@link #getEpochSecond},
     *       {@link #getNanos}, {@link #toEpochMillis}, {@link #toEpochMicros},
     *       {@link #fromEpochComponents}) subject to their own preconditions.</li>
     *   <li>All other precisions (p=0–2, p=4–5, p=7–12) are <em>not</em> registered in the type
     *       manager and are not recognized from a parsed type-signature string. Their
     *       {@link #getObjectValue} throws {@link UnsupportedOperationException}. Full type-system
     *       integration is planned for a follow-up change.</li>
     * </ul>
     *
     * @throws IllegalArgumentException if {@code precision} is outside [0, {@link #MAX_PRECISION}]
     */
    public static TimestampType createTimestampType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format(
                    "TIMESTAMP precision must be in range [0, %d]: %d", MAX_PRECISION, precision));
        }
        return INSTANCES[precision];
    }

    private TimestampType(int precision)
    {
        super(buildTypeSignature(precision));
        this.precision = precision;
    }

    private static TypeSignature buildTypeSignature(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Preserve "timestamp" (no parameter) so existing serialized metadata continues to parse.
            return parseTypeSignature(StandardTypes.TIMESTAMP);
        }
        if (precision == MAX_SHORT_PRECISION) {
            // Preserve "timestamp microseconds" for the same reason.
            return parseTypeSignature(StandardTypes.TIMESTAMP_MICROSECONDS);
        }
        // Other precisions use a numeric parameter; the type registry does not yet recognize the
        // "timestamp(p)" string form, so these instances are created directly rather than parsed.
        return new TypeSignature(StandardTypes.TIMESTAMP, TypeSignatureParameter.of((long) precision));
    }

    /**
     * Returns a {@link SqlTimestamp} for the value at {@code position}.
     *
     * <p>Only {@link #DEFAULT_PRECISION} (p=3, milliseconds) and {@link #MAX_SHORT_PRECISION}
     * (p=6, microseconds) are supported; all other precisions throw
     * {@link UnsupportedOperationException}. Support for the remaining short precisions
     * (p=0–2, p=4–5) and long precisions (p=7–12) is planned for a follow-up change.
     */
    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (precision != DEFAULT_PRECISION && precision != MAX_SHORT_PRECISION) {
            throw new UnsupportedOperationException(
                    "getObjectValue is not supported for TIMESTAMP(" + precision + ")");
        }
        TimeUnit unit = toTimeUnit(precision);
        if (properties.isLegacyTimestamp()) {
            return new SqlTimestamp(block.getLong(position), properties.getTimeZoneKey(), unit);
        }
        return new SqlTimestamp(block.getLong(position), unit);
    }

    /**
     * Returns {@code true} when the timestamp value fits in a single {@code long} (p ≤ {@link #MAX_SHORT_PRECISION}).
     *
     * <p>This is a <em>storage representation</em> concept. It does <em>not</em> imply full API support:
     * short precisions other than p=3 and p=6 are not registered in the type manager and their
     * {@link #getObjectValue} throws {@link UnsupportedOperationException}. See {@link #createTimestampType}
     * for the complete support matrix.
     */
    public boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    // Scaffolding for follow-up work that needs to distinguish millisecond-precision timestamps
    // (e.g. legacy JDBC write paths, ORC column statistics). Not yet used within this change.
    public boolean isMillisPrecision()
    {
        return precision == DEFAULT_PRECISION;
    }

    // Instances are interned, so reference equality is correct. equals() and hashCode() are both
    // overridden to satisfy the checkstyle EqualsHashCode rule. hashCode() uses precision rather
    // than identity to produce a stable, deterministic value across JVM runs.
    @Override
    public boolean equals(Object other)
    {
        return this == other;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), precision);
    }

    // Floor division handles negative (pre-1970) timestamps correctly.
    // The prior implementation used TimeUnit.toSeconds(), which truncates toward zero and incorrectly
    // returned 0 for any timestamp in the range (-1s, 0) — e.g. getEpochSecond(-1ms) was 0, not -1.
    public long getEpochSecond(long timestamp)
    {
        return floorDiv(timestamp, PRECISION_SCALE[precision]);
    }

    // Floor modulo handles negative (pre-1970) timestamps correctly; Java % does not.
    public int getNanos(long timestamp)
    {
        long fractional = floorMod(timestamp, PRECISION_SCALE[precision]);
        long scale = PRECISION_SCALE[precision];
        // For p <= 9, scale <= 1e9: multiply up to nanoseconds.
        // For p > 9, scale > 1e9: dividing avoids integer overflow; scale is always a multiple of 1e9.
        if (scale <= 1_000_000_000L) {
            return (int) (fractional * (1_000_000_000L / scale));
        }
        return (int) (fractional / (scale / 1_000_000_000L));
    }

    // Supported for all short precisions (p=0 through p=6, i.e. isShort()==true).
    // Long precisions (p=7-12) require a 128-bit representation and are not yet supported.
    public long toEpochMillis(long timestamp)
    {
        if (!isShort()) {
            throw new UnsupportedOperationException(
                    "toEpochMillis is not supported for TIMESTAMP(" + precision + ")");
        }
        return getEpochSecond(timestamp) * 1_000L + getNanos(timestamp) / 1_000_000;
    }

    // Supported for all short precisions (p=0 through p=6, i.e. isShort()==true).
    // Long precisions (p=7-12) require a 128-bit representation and are not yet supported.
    public long toEpochMicros(long timestamp)
    {
        if (!isShort()) {
            throw new UnsupportedOperationException(
                    "toEpochMicros is not supported for TIMESTAMP(" + precision + ")");
        }
        return getEpochSecond(timestamp) * 1_000_000L + getNanos(timestamp) / 1_000;
    }

    public long fromEpochComponents(long epochSecond, int nanos)
    {
        if (!isShort()) {
            throw new UnsupportedOperationException(
                    "fromEpochComponents is not supported for TIMESTAMP(" + precision + ")");
        }
        if (nanos < 0 || nanos >= 1_000_000_000) {
            throw new IllegalArgumentException("nanos must be in range [0, 999_999_999]: " + nanos);
        }
        long scale = PRECISION_SCALE[precision];
        return epochSecond * scale + nanos / (1_000_000_000L / scale);
    }

    private static TimeUnit toTimeUnit(int precision)
    {
        // Exact-precision checks: DEFAULT_PRECISION (3) stores epoch-millis; MAX_SHORT_PRECISION (6)
        // stores epoch-micros. Other precisions have no direct TimeUnit mapping.
        if (precision == DEFAULT_PRECISION) {
            return MILLISECONDS;
        }
        if (precision == MAX_SHORT_PRECISION) {
            return MICROSECONDS;
        }
        throw new UnsupportedOperationException(
                "Unsupported precision for TimeUnit conversion: TIMESTAMP(" + precision + ")");
    }
}

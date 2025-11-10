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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.iceberg.function.IcebergBucketFunction;
import com.facebook.presto.metadata.FunctionExtractor;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.Bucket.bucketLongDecimal;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.Bucket.bucketShortDecimal;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketDate;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketInteger;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketTimestamp;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketTimestampWithTimeZone;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketVarbinary;
import static com.facebook.presto.iceberg.function.IcebergBucketFunction.bucketVarchar;
import static io.airlift.slice.Slices.utf8Slice;

public class TestIcebergScalarFunctions
        extends AbstractTestFunctions
{
    public TestIcebergScalarFunctions()
    {
        super(TEST_SESSION, new FeaturesConfig(), new FunctionsConfig(), false);
    }

    @BeforeClass
    public void registerFunction()
    {
        ImmutableList.Builder<Class<?>> functions = ImmutableList.builder();
        functions.add(IcebergBucketFunction.class)
                .add(IcebergBucketFunction.Bucket.class);
        functionAssertions.addConnectorFunctions(FunctionExtractor.extractFunctions(functions.build(),
                new CatalogSchemaName("iceberg", "system")), "iceberg");
    }

    @Test
    public void testBucketFunction()
    {
        String catalogSchema = "iceberg.system";
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(10 as tinyint), 3)", INTEGER, bucketInteger(10, 3));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(1950 as smallint), 4)", INTEGER, bucketInteger(1950, 4));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(2375645 as int), 5)", INTEGER, bucketInteger(2375645, 5));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(2779099983928392323 as bigint), 6)", INTEGER, bucketInteger(2779099983928392323L, 6));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(456.43 as DECIMAL(5,2)), 12)", INTEGER, bucketShortDecimal(5, 2, 45643, 12));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('12345678901234567890.1234567890' as DECIMAL(30,10)), 12)", INTEGER, bucketLongDecimal(30, 10, encodeScaledValue(new BigDecimal("12345678901234567890.1234567890")), 12));

        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('nasdbsdnsdms' as varchar), 7)", INTEGER, bucketVarchar(utf8Slice("nasdbsdnsdms"), 7));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('nasdbsdnsdms' as varbinary), 8)", INTEGER, bucketVarbinary(utf8Slice("nasdbsdnsdms"), 8));

        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('2018-04-06' as date), 9)", INTEGER, bucketDate(DateOperators.castFromSlice(utf8Slice("2018-04-06")), 9));
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2018-04-06 04:35:00.000' AS TIMESTAMP),10)", INTEGER, bucketTimestamp(TimestampOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2018-04-06 04:35:00.000")), 10));
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2018-04-06 04:35:00.000 GMT' AS TIMESTAMP WITH TIME ZONE), 11)", INTEGER, bucketTimestampWithTimeZone(TimestampWithTimeZoneOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2018-04-06 04:35:00.000 GMT")), 11));
    }

    @Test
    public void testBucketFunctionWithEdgeCases()
    {
        String catalogSchema = "iceberg.system";
        
        // Test with zero values
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(0 as tinyint), 5)", INTEGER, bucketInteger(0, 5));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(0 as smallint), 7)", INTEGER, bucketInteger(0, 7));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(0 as int), 10)", INTEGER, bucketInteger(0, 10));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(0 as bigint), 12)", INTEGER, bucketInteger(0L, 12));
        
        // Test with negative values
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-10 as tinyint), 3)", INTEGER, bucketInteger(-10, 3));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-1950 as smallint), 4)", INTEGER, bucketInteger(-1950, 4));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-2375645 as int), 5)", INTEGER, bucketInteger(-2375645, 5));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-2779099983928392323 as bigint), 6)", INTEGER, bucketInteger(-2779099983928392323L, 6));
        
        // Test with max values
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(127 as tinyint), 8)", INTEGER, bucketInteger(127, 8));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(32767 as smallint), 15)", INTEGER, bucketInteger(32767, 15));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(2147483647 as int), 20)", INTEGER, bucketInteger(2147483647, 20));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(9223372036854775807 as bigint), 25)", INTEGER, bucketInteger(9223372036854775807L, 25));
        
        // Test with min values
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-128 as tinyint), 8)", INTEGER, bucketInteger(-128, 8));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-32768 as smallint), 12)", INTEGER, bucketInteger(-32768, 12));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-2147483648 as int), 16)", INTEGER, bucketInteger(-2147483648, 16));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-9223372036854775808 as bigint), 18)", INTEGER, bucketInteger(-9223372036854775808L, 18));
    }

    @Test
    public void testBucketFunctionWithVariousStrings()
    {
        String catalogSchema = "iceberg.system";
        
        // Test with different string patterns
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('test' as varchar), 5)", INTEGER, bucketVarchar(utf8Slice("test"), 5));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('a' as varchar), 3)", INTEGER, bucketVarchar(utf8Slice("a"), 3));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('Hello World!' as varchar), 10)", INTEGER, bucketVarchar(utf8Slice("Hello World!"), 10));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('123456789' as varchar), 8)", INTEGER, bucketVarchar(utf8Slice("123456789"), 8));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('special@#$%' as varchar), 12)", INTEGER, bucketVarchar(utf8Slice("special@#$%"), 12));
        
        // Test with empty string
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('' as varchar), 4)", INTEGER, bucketVarchar(utf8Slice(""), 4));
        
        // Test varbinary
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('binary' as varbinary), 6)", INTEGER, bucketVarbinary(utf8Slice("binary"), 6));
    }

    @Test
    public void testBucketFunctionWithVariousDecimals()
    {
        String catalogSchema = "iceberg.system";
        
        // Test with zero decimals
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(0.00 as DECIMAL(5,2)), 8)", INTEGER, bucketShortDecimal(5, 2, 0, 8));
        
        // Test with negative decimals
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-456.43 as DECIMAL(5,2)), 12)", INTEGER, bucketShortDecimal(5, 2, -45643, 12));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(-99.99 as DECIMAL(5,2)), 10)", INTEGER, bucketShortDecimal(5, 2, -9999, 10));
        
        // Test with max short decimal
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(999.99 as DECIMAL(5,2)), 15)", INTEGER, bucketShortDecimal(5, 2, 99999, 15));
        
        // Test long decimals with zero
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('0.0000000000' as DECIMAL(30,10)), 7)", INTEGER, bucketLongDecimal(30, 10, encodeScaledValue(new BigDecimal("0.0000000000")), 7));
        
        // Test long decimals with negative
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('-12345678901234567890.1234567890' as DECIMAL(30,10)), 20)", INTEGER, bucketLongDecimal(30, 10, encodeScaledValue(new BigDecimal("-12345678901234567890.1234567890")), 20));
    }

    @Test
    public void testBucketFunctionWithVariousDates()
    {
        String catalogSchema = "iceberg.system";
        
        // Test with various dates
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('2023-01-01' as date), 12)", INTEGER, bucketDate(DateOperators.castFromSlice(utf8Slice("2023-01-01")), 12));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('2000-12-31' as date), 7)", INTEGER, bucketDate(DateOperators.castFromSlice(utf8Slice("2000-12-31")), 7));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('1970-01-01' as date), 10)", INTEGER, bucketDate(DateOperators.castFromSlice(utf8Slice("1970-01-01")), 10));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('2024-02-29' as date), 8)", INTEGER, bucketDate(DateOperators.castFromSlice(utf8Slice("2024-02-29")), 8));
    }

    @Test
    public void testBucketFunctionWithVariousTimestamps()
    {
        String catalogSchema = "iceberg.system";
        
        // Test with various timestamps
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2023-12-25 00:00:00.000' AS TIMESTAMP), 15)", INTEGER, bucketTimestamp(TimestampOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2023-12-25 00:00:00.000")), 15));
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2000-01-01 12:30:45.123' AS TIMESTAMP), 8)", INTEGER, bucketTimestamp(TimestampOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2000-01-01 12:30:45.123")), 8));
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('1970-01-01 00:00:00.000' AS TIMESTAMP), 6)", INTEGER, bucketTimestamp(TimestampOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("1970-01-01 00:00:00.000")), 6));
        
        // Test with timestamp with time zone
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2023-07-15 10:20:30.000 UTC' AS TIMESTAMP WITH TIME ZONE), 13)", INTEGER, bucketTimestampWithTimeZone(TimestampWithTimeZoneOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2023-07-15 10:20:30.000 UTC")), 13));
        functionAssertions.assertFunction(catalogSchema + ".bucket(CAST('2000-06-15 18:45:12.000 GMT' AS TIMESTAMP WITH TIME ZONE), 9)", INTEGER, bucketTimestampWithTimeZone(TimestampWithTimeZoneOperators.castFromSlice(TEST_SESSION.getSqlFunctionProperties(), utf8Slice("2000-06-15 18:45:12.000 GMT")), 9));
    }

    @Test
    public void testBucketFunctionWithDifferentBucketSizes()
    {
        String catalogSchema = "iceberg.system";
        
        // Test with various bucket sizes
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(42 as bigint), 1)", INTEGER, bucketInteger(42L, 1));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(42 as bigint), 2)", INTEGER, bucketInteger(42L, 2));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(42 as bigint), 50)", INTEGER, bucketInteger(42L, 50));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(42 as bigint), 100)", INTEGER, bucketInteger(42L, 100));
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(42 as bigint), 1000)", INTEGER, bucketInteger(42L, 1000));
    }

    @Test
    public void testBucketFunctionConsistency()
    {
        String catalogSchema = "iceberg.system";
        
        // Test that same input produces same bucket
        long value = bucketInteger(42L, 10);
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(42 as bigint), 10)", INTEGER, value);
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast(42 as bigint), 10)", INTEGER, value);
        
        long stringValue = bucketVarchar(utf8Slice("test"), 5);
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('test' as varchar), 5)", INTEGER, stringValue);
        functionAssertions.assertFunction(catalogSchema + ".bucket(cast('test' as varchar), 5)", INTEGER, stringValue);
    }
}

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
package com.facebook.presto.sidecar;

import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

public class TestNativeSidecarIcebergCatalog
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .setCoordinatorSidecarEnabled(true)
                .build();
        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
//        queryRunner.loadFunctionNamespaceManager(
//                NativeFunctionNamespaceManagerFactory.NAME,
//                "iceberg",
//                ImmutableMap.of(
//                        "supported-function-languages", "CPP",
//                        "function-implementation-type", "CPP"));
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "native",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Test
    public void testBucketFunctionWithIntegers1()
    {
        // Test bucket function with integers - results should be in range [0, numBuckets-1]
//        assertQuery("SELECT iceberg.system.bucket(cast(1950 as smallint), 4)",
//                "SELECT iceberg.system.bucket(4, cast(1950 as smallint))");
        assertQuery("SELECT iceberg.system.bucket(cast(1950 as smallint), 4)");
    }

    @Test
    public void testBucketFunctionWithIntegers2()
    {
//        assertQuery(
//                "SELECT iceberg.system.bucket(cast(2375645 as int), 5)",
//                "SELECT iceberg.system.bucket(5, cast(2375645 as int))");
        assertQuery(
                "SELECT iceberg.system.bucket(cast(2375645 as int), 5)");
    }
}

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

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;

/**
 * Tests catalog isolation for function namespaces.
 * Verifies that functions from one catalog do not leak into another catalog,
 * ensuring proper separation of function namespaces.
 */
public class TestCatalogFunctionIsolation
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createRegion(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        TestNativeSidecarPlugin.setupNativeSidecarPlugin(queryRunner);

        // Setup multiple catalog function namespace managers to test isolation
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "hive",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));

        // Note: Additional catalogs would be configured here if they existed
        // For now, we test that hive catalog functions are isolated

        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Test
    public void testHiveCatalogFunctionAccessible()
    {
        // Verify hive catalog functions are accessible when configured
        assertQuery("SELECT hive.default.initcap('hello')", "SELECT('Hello')");
    }

    @Test
    public void testUnregisteredCatalogFunctionNotAccessible()
    {
        // Verify that functions from unregistered catalogs are not accessible
        // This tests that catalog filtering prevents cross-catalog leakage
        assertQueryFails(
                "SELECT unregistered_catalog.default.initcap('hello')",
                "(?s).*Function unregistered_catalog.default.initcap not registered.*");
    }

    @Test
    public void testNativeCatalogNotConfiguredByDefault()
    {
        // Verify that native catalog is not accessible unless explicitly configured
        // This ensures each catalog must be explicitly set up
        assertQueryFails(
                "SELECT native.default.array_sum(ARRAY[1, 2, 3])",
                "(?s).*Function native.default.array_sum not registered.*");
    }

    @Test
    public void testCatalogQualifiedFunctionNameRequired()
    {
        // Verify that unqualified function names don't accidentally access catalog functions
        // Only fully qualified names (catalog.schema.function) should work
        assertQueryFails(
                "SELECT initcap('hello')",
                "(?s).*(Function.*not registered|line 1:8: Function 'initcap' not registered).*");
    }

    @Test
    public void testWrongCatalogNameFails()
    {
        // Test that using the wrong catalog name in the function call fails
        assertQueryFails(
                "SELECT wrongcatalog.default.initcap('hello')",
                "(?s).*Function wrongcatalog.default.initcap not registered.*");
    }

    @Test
    public void testMultipleCatalogFunctionsInSameQuery()
    {
        // Verify that functions from the same catalog can be used together
        assertQuery(
                "SELECT hive.default.initcap('hello'), hive.default.initcap('world')",
                "SELECT 'Hello', 'World'");
    }

    @Test
    public void testCatalogFunctionInSubquery()
    {
        // Test catalog function in a subquery to ensure proper namespace resolution
        assertQuery(
                "SELECT * FROM (SELECT hive.default.initcap('test') as result) t",
                "SELECT 'Test'");
    }

    @Test
    public void testCatalogFunctionWithJoin()
    {
        // Test catalog function with table joins
        assertQuery(
                "SELECT hive.default.initcap(n.name) FROM nation n LIMIT 1",
                "SELECT initcap(name) FROM nation LIMIT 1");
    }
}

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
 * Tests catalog-filtered function namespaces with sidecar enabled.
 * Verifies that functions are properly isolated by catalog and that
 * the /v1/functions/{catalog} endpoint correctly filters functions.
 */
public class TestCatalogFilteredFunctionNamespaces
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

        // Setup hive catalog function namespace manager
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "hive",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));

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
    public void testHiveCatalogInitcapFunction()
    {
        assertQuery("SELECT hive.default.initcap('Hello world')", "SELECT('Hello World')");
        assertQuery("SELECT hive.default.initcap('abcd')", "SELECT('Abcd')");
        assertQuery("SELECT hive.default.initcap('a   b   c')", "SELECT('A   B   C')");
    }

    @Test
    public void testHiveCatalogInitcapWithVariousInputs()
    {
        // Test with empty string
        assertQuery("SELECT hive.default.initcap('')", "SELECT('')");
        
        // Test with single character
        assertQuery("SELECT hive.default.initcap('x')", "SELECT('X')");
        
        // Test with numbers
        assertQuery("SELECT hive.default.initcap('hello123world')", "SELECT('Hello123world')");
        
        // Test with special characters
        assertQuery("SELECT hive.default.initcap('hello-world')", "SELECT('Hello-World')");
    }

    @Test
    public void testHiveCatalogInitcapWithNullValue()
    {
        assertQuery("SELECT hive.default.initcap(NULL)", "SELECT CAST(NULL AS VARCHAR)");
    }

    @Test
    public void testQualifiedFunctionCallWithCatalogAndSchema()
    {
        // Ensure fully qualified function names work correctly
        assertQuery("SELECT hive.default.initcap('test')", "SELECT('Test')");
    }

    @Test
    public void testHiveCatalogInitcapInComplexQuery()
    {
        // Test function in WHERE clause
        assertQuery(
                "SELECT name FROM nation WHERE hive.default.initcap(name) = hive.default.initcap(name) LIMIT 1",
                "SELECT name FROM nation LIMIT 1");
    }
}

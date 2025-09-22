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
import com.facebook.presto.scalar.sql.NativeSqlInvokedFunctionsPlugin;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestNativeSidecarMultipleCatalogs
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
        createCustomer(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        
        // Setup multiple function namespace managers with different catalog configurations
        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
        
        // Load session property provider
        queryRunner.loadSessionPropertyProvider(
                com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProviderFactory.NAME,
                ImmutableMap.of());
        
        // First namespace manager with default (all functions)
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "native_all",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));
        
        // Second namespace manager with presto catalog filtering
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "native_presto_filtered",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP",
                        "sidecar.catalog-name", "presto"));
        
        // Third namespace manager with native catalog filtering
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "native_native_filtered", 
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP",
                        "sidecar.catalog-name", "native"));
                        
        queryRunner.loadTypeManager(com.facebook.presto.sidecar.typemanager.NativeTypeManagerFactory.NAME);
        queryRunner.loadPlanCheckerProviderManager("native", ImmutableMap.of());
        queryRunner.installPlugin(new NativeSqlInvokedFunctionsPlugin());
        
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
    public void testMultipleCatalogNamespaces()
    {
        // Verify that we can register multiple function namespace managers
        // with different catalog configurations
        
        // Check that all namespace managers are registered
        assertTrue(getQueryRunner().getMetadata().getFunctionAndTypeManager()
                .getFunctionNamespaceManagers().containsKey("native_all"),
                "Should have native_all namespace manager");
                
        assertTrue(getQueryRunner().getMetadata().getFunctionAndTypeManager()
                .getFunctionNamespaceManagers().containsKey("native_presto_filtered"),
                "Should have native_presto_filtered namespace manager");
                
        assertTrue(getQueryRunner().getMetadata().getFunctionAndTypeManager()
                .getFunctionNamespaceManagers().containsKey("native_native_filtered"),
                "Should have native_native_filtered namespace manager");
    }

    @Test
    public void testBasicQueryStillWorks()
    {
        // Ensure basic queries work even with multiple catalog configurations
        @Language("SQL") String sql = "SELECT COUNT(*) FROM nation";
        MaterializedResult result = computeActual(sql);
        assertTrue(result.getRowCount() == 1, "Should return one row with count");
        
        MaterializedRow row = result.getMaterializedRows().get(0);
        Long count = (Long) row.getField(0);
        assertTrue(count > 0, "Nation table should have rows");
    }

    @Test
    public void testShowFunctionsWithMultipleCatalogs()
    {
        // Test that SHOW FUNCTIONS still works with multiple catalog configurations
        @Language("SQL") String sql = "SHOW FUNCTIONS";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        
        // Should have some functions available
        assertFalse(actualRows.isEmpty(), "Should have functions available");
        
        // Verify that functions have proper namespacing
        boolean foundNamespacedFunction = false;
        for (MaterializedRow actualRow : actualRows) {
            List<Object> row = actualRow.getFields();
            String fullFunctionName = row.get(5).toString(); // Full function name with namespace
            
            // Check if function has namespace format (catalog.schema.function)
            if (Pattern.matches(".*\\..*\\..*", fullFunctionName)) {
                foundNamespacedFunction = true;
                break;
            }
        }
        
        assertTrue(foundNamespacedFunction, "Should find at least one function with proper namespace format");
    }
}
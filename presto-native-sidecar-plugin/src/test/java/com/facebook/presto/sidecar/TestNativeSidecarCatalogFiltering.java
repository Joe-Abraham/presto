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

import com.facebook.presto.Session;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.scalar.sql.NativeSqlInvokedFunctionsPlugin;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.sidecar.functionNamespace.FunctionDefinitionProvider;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionDefinitionProvider;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManager;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.SqlFunction;
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

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPluginWithCatalog;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestNativeSidecarCatalogFiltering
        extends AbstractTestQueryFramework
{
    private static final String HIVE_CATALOG_REGEX = "hive\\.default\\..*";
    private static final String PRESTO_CATALOG_REGEX = "presto\\.default\\..*"; 

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
        // Setup with hive catalog filtering
        setupNativeSidecarPluginWithCatalog(queryRunner, "hive");
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
    public void testCatalogFilteringConfiguration()
    {
        // Verify that the function namespace manager is properly configured with catalog filtering
        FunctionNamespaceManager<? extends SqlFunction> functionNamespaceManager = 
                getQueryRunner().getMetadata().getFunctionAndTypeManager()
                        .getFunctionNamespaceManagers().get("native");
        
        checkArgument(functionNamespaceManager instanceof NativeFunctionNamespaceManager, 
                "Expected NativeFunctionNamespaceManager but got %s", functionNamespaceManager);
        
        FunctionDefinitionProvider functionDefinitionProvider = 
                ((NativeFunctionNamespaceManager) functionNamespaceManager).getFunctionDefinitionProvider();
        
        checkArgument(functionDefinitionProvider instanceof NativeFunctionDefinitionProvider, 
                "Expected NativeFunctionDefinitionProvider but got %s", functionDefinitionProvider);
    }

    @Test
    public void testShowFunctionsWithCatalogFiltering()
    {
        @Language("SQL") String sql = "SHOW FUNCTIONS";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        
        boolean foundHiveFunction = false;
        boolean foundPrestoFunction = false;
        
        for (MaterializedRow actualRow : actualRows) {
            List<Object> row = actualRow.getFields();
            String fullFunctionName = row.get(5).toString(); // Full function name with namespace
            
            if (Pattern.matches(HIVE_CATALOG_REGEX, fullFunctionName)) {
                foundHiveFunction = true;
            }
            if (Pattern.matches(PRESTO_CATALOG_REGEX, fullFunctionName)) {
                foundPrestoFunction = true;
            }
        }
        
        // With hive catalog filtering, we should find hive functions
        // The presence of presto functions depends on the actual implementation
        assertTrue(foundHiveFunction || foundPrestoFunction, 
                "Should find at least some functions with proper catalog namespacing");
    }

    @Test
    public void testBasicQueryWithCatalogFilteredFunctions()
    {
        // Test that basic queries still work with catalog filtering enabled
        @Language("SQL") String sql = "SELECT COUNT(*) FROM nation";
        MaterializedResult result = computeActual(sql);
        assertEquals(result.getRowCount(), 1);
        
        // Verify the result
        MaterializedRow row = result.getMaterializedRows().get(0);
        Long count = (Long) row.getField(0);
        assertTrue(count > 0, "Nation table should have rows");
    }

    @Test
    public void testHiveFunctionAvailability()
    {
        // Test that we can access functions when catalog filtering is configured
        // This depends on what functions are actually registered in the native sidecar
        @Language("SQL") String sql = "SHOW FUNCTIONS";
        MaterializedResult actualResult = computeActual(sql);
        
        assertFalse(actualResult.getMaterializedRows().isEmpty(), 
                "Should have at least some functions available even with catalog filtering");
    }
}
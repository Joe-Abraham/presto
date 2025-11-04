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
package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * End-to-end test for remote functions in native execution environment.
 * Similar to TestRestRemoteFunctions but adapted for container-based native execution.
 * Tests call remote functions served by the Presto Function Server.
 */
public class TestNativeRemoteFunctionsE2E
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner(
                ContainerQueryRunner.DEFAULT_COORDINATOR_PORT,
                ContainerQueryRunner.TPCH_CATALOG,
                ContainerQueryRunner.TINY_SCHEMA,
                ContainerQueryRunner.DEFAULT_NUMBER_OF_WORKERS,
                ContainerQueryRunner.DEFAULT_FUNCTION_SERVER_PORT,
                true);
    }

    @Test
    public void testShowFunction()
    {
        MaterializedResult actualResult = computeActual("show functions like '%remote.%'");
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        assertFalse(actualRows.isEmpty(), "Expected at least one function matching 'remote.%', but found none.");
    }

    @Test
    public void testRemoteFunctions()
    {
        // Test various remote functions with different argument types
        assertEquals(
                computeActual("select remote.default.abs(-1230)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "1230");
        assertEquals(
                computeActual("select remote.default.day(interval '2' day)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual("select remote.default.length(CAST('AB' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual("select remote.default.floor(100000.99)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "100000.0");
        assertEquals(
                computeActual("select remote.default.to_base32(CAST('abc' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "MFRGG===");
    }

    @Test
    public void testFunctionPlugins()
    {
        // Validate that remote functions are available and can be discovered
        // Note: Unlike TestRestRemoteFunctions, we cannot dynamically install plugins
        // in the container-based function server at runtime.
        MaterializedResult actualResult = computeActual("show functions like '%remote.default.abs%'");
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        assertFalse(actualRows.isEmpty(), "Expected at least one function matching 'remote.default.abs%', but found none.");
    }

    @Test
    public void testRemoteFunctionAppliedToColumn()
    {
        // Verify remote functions work correctly on actual table columns
        assertEquals(computeActual("SELECT remote.default.floor(o_totalprice) FROM tpch.sf1.orders")
                .getMaterializedRows().size(), 1500000);
        assertEquals(computeActual("SELECT remote.default.abs(l_discount) FROM tpch.sf1.lineitem")
                .getMaterializedRows().size(), 6001215);

        // Verify results match built-in function results
        assertQueryWithSameQueryRunner(
                "SELECT remote.default.floor(o_totalprice) FROM tpch.sf1.orders",
                "SELECT floor(o_totalprice) FROM tpch.sf1.orders");
        assertQueryWithSameQueryRunner(
                "SELECT remote.default.abs(l_discount) FROM tpch.sf1.lineitem",
                "SELECT abs(l_discount) FROM tpch.sf1.lineitem");

        assertEquals(computeActual("SELECT remote.default.length(CAST(o_comment AS VARBINARY)) FROM tpch.sf1.orders")
                .getMaterializedRows().size(), 1500000);
    }
}

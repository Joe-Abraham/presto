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

import com.facebook.presto.Session;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.server.TestingFunctionServer;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * End-to-end test for remote functions in native execution environment.
 * Similar to TestRestRemoteFunctions but adapted for native execution with REST-based function server.
 * Tests call remote functions served by the Presto Function Server.
 */
public class TestNativeRemoteFunctionsE2E
        extends AbstractTestQueryFramework
{
    private TestingFunctionServer functionServer;
    private int functionServerPort;

    private static final Session session = testSessionBuilder()
            .setSource("test")
            .setCatalog("hive")
            .setSchema("tpch")
            .setSystemProperty("remote_functions_enabled", "true")
            .build();

    private static int findRandomPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        functionServerPort = findRandomPort();
        functionServer = new TestingFunctionServer(functionServerPort);

        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(false)
                .build();

        // Install function namespace manager plugin for REST-based remote functions
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        queryRunner.loadFunctionNamespaceManager(
                RestBasedFunctionNamespaceManagerFactory.NAME,
                "rest",
                ImmutableMap.of(
                        "supported-function-languages", "JAVA",
                        "function-implementation-type", "REST",
                        "rest-based-function-manager.rest.url", format("http://localhost:%s", functionServerPort)));

        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected Session getSession()
    {
        return session;
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        NativeQueryRunnerUtils.createLineitem(queryRunner);
        NativeQueryRunnerUtils.createOrders(queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        if (functionServer != null) {
            // Function server cleanup if needed
            functionServer = null;
        }
    }

    @Test
    public void testShowFunction()
    {
        MaterializedResult actualResult = computeActual(session, "show functions like '%rest.%'");
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        assertFalse(actualRows.isEmpty(), "Expected at least one function matching 'rest.%', but found none.");
    }

    @Test
    public void testRemoteFunctions()
    {
        // Test various remote functions with different argument types
        assertEquals(
                computeActual(session, "select rest.default.abs(-1230)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "1230");
        assertEquals(
                computeActual(session, "select rest.default.day(interval '2' day)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual(session, "select rest.default.length(CAST('AB' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
        assertEquals(
                computeActual(session, "select rest.default.floor(100000.99)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "100000.0");
    }

    @Test
    public void testFunctionPlugins()
    {
        // Install a custom plugin and validate it can be discovered
        functionServer.installPlugin(new DummyPlugin());
        MaterializedResult actualResult = computeActual(session, "show functions like '%rest.default.is_positive%'");
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        assertFalse(actualRows.isEmpty());

        assertEquals(
                computeActual(session, "SELECT rest.default.is_positive(1)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "true");
        assertEquals(
                computeActual(session, "SELECT rest.default.is_positive(-1)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "false");
    }

    @Test
    public void testRemoteFunctionAppliedToColumn()
    {
        assertQueryWithSameQueryRunner(
                "SELECT rest.default.floor(totalprice) FROM orders",
                "SELECT floor(totalprice) FROM orders");
        assertQueryWithSameQueryRunner(
                "SELECT rest.default.abs(discount) FROM lineitem",
                "SELECT abs(discount) FROM lineitem");
    }

    private static final class DummyPlugin
            implements Plugin
    {
        @Override
        public Set<Class<?>> getFunctions()
        {
            return ImmutableSet.of(DummyFunctions.class);
        }
    }

    public static class DummyFunctions
    {
        @ScalarFunction
        @Description("Check if number is positive")
        @SqlType(BOOLEAN)
        public static boolean isPositive(@SqlType(BIGINT) long input)
        {
            return input > 0;
        }
    }
}

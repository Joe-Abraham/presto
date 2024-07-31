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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static org.testng.Assert.fail;

public class ContainerQueryRunner
        implements QueryRunner
{
    private static final Network network = Network.newNetwork();
    private static final String PRESTO_COORDINATOR_IMAGE = System.getProperty("coordinatorImage", "presto-coordinator:latest");
    private static final String PRESTO_WORKER_IMAGE = System.getProperty("workerImage", "presto-worker:latest");
    private static final String BASE_DIR = System.getProperty("user.dir");
    private static final String CONTAINER_TIMEOUT = System.getProperty("containerTimeout", "120");
    private static final String CLUSTER_SHUTDOWN_TIMEOUT = System.getProperty("clusterShutDownTimeout", "10");
    private static final String CATALOG = System.getProperty("catalog", "tpch");
    private static final String SCHEMA = System.getProperty("schema", "tiny");
    private final GenericContainer<?> coordinator;
    private final GenericContainer<?> worker1;
    private final GenericContainer<?> worker2;
    private final GenericContainer<?> worker3;
    private final GenericContainer<?> worker4;

    public ContainerQueryRunner()
    {
        coordinator = new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(8081)
                .withNetwork(network).withNetworkAliases("presto-coordinator")
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/etc", "/opt/presto-server/etc", BindMode.READ_WRITE)
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(Long.parseLong(CONTAINER_TIMEOUT)));

        worker1 = createWorker(7777, "native-worker-1");
        worker2 = createWorker(7778, "native-worker-2");
        worker3 = createWorker(7779, "native-worker-3");
        worker4 = createWorker(7780, "native-worker-4");

        coordinator.start();
        worker1.start();
        worker2.start();
        worker3.start();
        worker4.start();

        System.out.println("Presto UI is accessible at http://localhost:" + coordinator.getMappedPort(8081));

        try {
            TimeUnit.SECONDS.sleep(5);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static MaterializedResult toMaterializedResult(String csvData)
    {
        List<MaterializedRow> rows = new ArrayList<>();
        List<Type> columnTypes = new ArrayList<>();

        // Split the CSV data into lines
        String[] lines = csvData.split("\n");

        // Parse all rows and collect them
        List<String[]> allRows = parseCsvLines(lines);

        // Infer column types based on the maximum columns found
        int maxColumns = allRows.stream().mapToInt(row -> row.length).max().orElse(0);
        for (int i = 0; i < maxColumns; i++) {
            final int columnIndex = i; // Make index effectively final
            columnTypes.add(inferType(allRows.stream().map(row -> columnIndex < row.length ? row[columnIndex] : "").collect(Collectors.toList())));
        }

        // Convert all rows to MaterializedRow
        for (String[] columns : allRows) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < columnTypes.size(); i++) {
                values.add(i < columns.length ? convertToType(columns[i], columnTypes.get(i)) : null);
            }
            rows.add(new MaterializedRow(5, values));
        }

        // Create and return the MaterializedResult
        return new MaterializedResult(rows, columnTypes);
    }

    private static List<String[]> parseCsvLines(String[] lines)
    {
        List<String[]> allRows = new ArrayList<>();
        List<String> currentRow = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideQuotes = false;

        for (String line : lines) {
            for (int i = 0; i < line.length(); i++) {
                char ch = line.charAt(i);
                if (ch == '"') {
                    // Handle double quotes inside quoted string
                    if (insideQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        currentField.append(ch);
                        i++;
                    }
                    else {
                        insideQuotes = !insideQuotes;
                    }
                }
                else if (ch == ',' && !insideQuotes) {
                    currentRow.add(currentField.toString());
                    currentField.setLength(0); // Clear the current field
                }
                else {
                    currentField.append(ch);
                }
            }
            if (insideQuotes) {
                currentField.append('\n'); // Add newline for multiline fields
            }
            else {
                currentRow.add(currentField.toString());
                currentField.setLength(0); // Clear the current field
                allRows.add(currentRow.toArray(new String[0]));
                currentRow.clear();
            }
        }
        if (!currentRow.isEmpty()) {
            currentRow.add(currentField.toString());
            allRows.add(currentRow.toArray(new String[0]));
        }
        return allRows;
    }

    private static Type inferType(List<String> values)
    {
        boolean isBigint = true;
        boolean isDouble = true;
        boolean isBoolean = true;

        for (String value : values) {
            if (!value.matches("^-?\\d+$")) {
                isBigint = false;
            }
            if (!value.matches("^-?\\d+\\.\\d+$")) {
                isDouble = false;
            }
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                isBoolean = false;
            }
        }

        if (isBigint) {
            return BigintType.BIGINT;
        }
        else if (isDouble) {
            return DoubleType.DOUBLE;
        }
        else if (isBoolean) {
            return BooleanType.BOOLEAN;
        }
        else {
            return VarcharType.VARCHAR;
        }
    }

    private static Object convertToType(String value, Type type)
    {
        if (value.isEmpty()) {
            return null;
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return value;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return Long.parseLong(value);
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return Double.parseDouble(value);
        }
        else if (type.equals(BooleanType.BOOLEAN)) {
            return Boolean.parseBoolean(value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private Path createTempNodeProperties(String nodeId)
            throws IOException
    {
        Path originalNodeProperties = Paths.get(BASE_DIR + "/testcontainers/nativeworker/velox-etc/node.properties");
        Path tempNodeProperties = Files.createTempFile("node", ".properties");

        // Copy original content to temp file
        Files.copy(originalNodeProperties, tempNodeProperties, StandardCopyOption.REPLACE_EXISTING);

        // Append node.id to the temp file
        String nodeIdEntry = "node.id=" + nodeId + System.lineSeparator();
        Files.write(tempNodeProperties, nodeIdEntry.getBytes(), StandardOpenOption.APPEND);

        return tempNodeProperties;
    }

    private GenericContainer<?> createWorker(int port, String nodeId) {
        Path tempNodeProperties = null;
        try {
            tempNodeProperties = createTempNodeProperties(nodeId);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(port)
                .withNetwork(network)
                .withNetworkAliases("presto-worker")
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/velox-etc/config.properties", "/opt/presto-server/etc/config.properties", BindMode.READ_ONLY)
                .withFileSystemBind(tempNodeProperties.toString(), "/opt/presto-server/etc/node.properties", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/velox-etc/catalog/tpch.properties", "/opt/presto-server/etc/catalog/tpch.properties", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));
    }


    public Container.ExecResult executeQuery(String sql)
    {
        String[] command = {
                "/opt/presto-cli",
                "--server",
                "presto-coordinator:8081",
                "--catalog",
                CATALOG,
                "--schema",
                SCHEMA,
                "--execute",
                sql
        };

        Container.ExecResult execResult = null;
        try {
            execResult = coordinator.execInContainer(command);
        }
        catch (IOException e) {
            fail("Presto CLI failed with error message: " + e.getMessage());
        }
        catch (InterruptedException e) {
            fail("Presto CLI failed with error message: " + e.getMessage());
        }

        if (execResult.getExitCode() != 0) {
            String errorDetails = "Stdout: " + execResult.getStdout() + "\nStderr: " + execResult.getStderr();
            fail("Presto CLI exited with error code: " + execResult.getExitCode() + "\n" + errorDetails);
        }
        return execResult;
    }

    @Override
    public void close()
    {
        try {
            TimeUnit.SECONDS.sleep(Long.parseLong(CLUSTER_SHUTDOWN_TIMEOUT));
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        coordinator.stop();
        worker1.stop();
        worker2.stop();
        worker3.stop();
        worker4.stop();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Metadata getMetadata()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SplitManager getSplitManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<EventListener> getEventListener()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedResult execute(String sql)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedResult execute(Session session, String sql, List<? extends Type> resultTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock getExclusiveLock()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNodeCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Session getDefaultSession()
    {
        return null;
    }

    @Override
    public MaterializedResult execute(Session session, String sql)
    {
        Container.ExecResult execResult = executeQuery(sql);
        return toMaterializedResult(execResult.getStdout());
    }
}

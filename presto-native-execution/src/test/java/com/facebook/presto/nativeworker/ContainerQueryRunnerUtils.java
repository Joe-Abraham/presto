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

import au.com.bytecode.opencsv.CSVReader;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.testng.FileAssert.fail;

public class ContainerQueryRunnerUtils
{
    static Logger logger = Logger.getLogger(ContainerQueryRunnerUtils.class.getName());

    private ContainerQueryRunnerUtils() {}

    public static void createCoordinatorTpchProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("connector.name", "tpch");
        properties.setProperty("tpch.column-naming", "STANDARD");
        createPropertiesFile("testcontainers/coordinator/etc/catalog/tpch.properties", properties);
    }

    public static void createNativeWorkerTpchProperties(String nodeId)
    {
        Properties properties = new Properties();
        properties.setProperty("connector.name", "tpch");
        properties.setProperty("tpch.column-naming", "STANDARD");
        createPropertiesFile("testcontainers/" + nodeId + "/etc/catalog/tpch.properties", properties);
    }

    public static void createNativeWorkerConfigProperties(int coordinatorPort, String nodeId)
    {
        Properties properties = new Properties();
        properties.setProperty("presto.version", "testversion");
        properties.setProperty("http-server.http.port", "7777");
        properties.setProperty("discovery.uri", "http://presto-coordinator:" + coordinatorPort);
        properties.setProperty("system-memory-gb", "2");
        properties.setProperty("native.sidecar", "false");
        createPropertiesFile("testcontainers/" + nodeId + "/etc/config.properties", properties);
    }

    public static void createCoordinatorConfigProperties(int port)
    {
        Properties properties = new Properties();
        properties.setProperty("coordinator", "true");
        properties.setProperty("presto.version", "testversion");
        properties.setProperty("node-scheduler.include-coordinator", "false");
        properties.setProperty("http-server.http.port", Integer.toString(port));
        properties.setProperty("discovery-server.enabled", "true");
        properties.setProperty("discovery.uri", "http://presto-coordinator:" + port);

        // Get native worker system properties and add them to the coordinator properties
        Map<String, String> nativeWorkerProperties = NativeQueryRunnerUtils.getNativeWorkerSystemProperties();
        for (Map.Entry<String, String> entry : nativeWorkerProperties.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        createPropertiesFile("testcontainers/coordinator/etc/config.properties", properties);
    }

    public static void createCoordinatorJvmConfig()

    {
        String jvmConfig = "-server\n" +
                "-Xmx1G\n" +
                "-XX:+UseG1GC\n" +
                "-XX:G1HeapRegionSize=32M\n" +
                "-XX:+UseGCOverheadLimit\n" +
                "-XX:+ExplicitGCInvokesConcurrent\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+ExitOnOutOfMemoryError\n" +
                "-Djdk.attach.allowAttachSelf=true\n";
        createScriptFile("testcontainers/coordinator/etc/jvm.config", jvmConfig);
    }

    public static void createCoordinatorLogProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("com.facebook.presto", "DEBUG");
        createPropertiesFile("testcontainers/coordinator/etc/log.properties", properties);
    }

    public static void createCoordinatorNodeProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("node.environment", "testing");
        properties.setProperty("node.location", "testing-location");
        properties.setProperty("node.data-dir", "/var/lib/presto/data");
        createPropertiesFile("testcontainers/coordinator/etc/node.properties", properties);
    }

    public static void createNativeWorkerNodeProperties(String nodeId)
    {
        Properties properties = new Properties();
        properties.setProperty("node.environment", "testing");
        properties.setProperty("node.location", "testing-location");
        properties.setProperty("node.id", nodeId);
        createPropertiesFile("testcontainers/" + nodeId + "/etc/node.properties", properties);
    }

    public static void createNativeWorkerVeloxProperties(String nodeId)
    {
        Properties properties = new Properties();
        properties.setProperty("mutable-config", "true");
        createPropertiesFile("testcontainers/" + nodeId + "/etc/velox.properties", properties);
    }

    public static void createCoordinatorEntryPointScript()
    {
        String scriptContent = "#!/bin/sh\n" +
                "set -e\n" +
                "$PRESTO_HOME/bin/launcher run\n";
        createScriptFile("testcontainers/coordinator/entrypoint.sh", scriptContent);
    }

    public static void createNativeWorkerEntryPointScript(String nodeId)
    {
        String scriptContent = "#!/bin/sh\n\n" +
                "GLOG_logtostderr=1 presto_server \\\n" +
                "    --etc-dir=/opt/presto-server/etc\n";
        createScriptFile("testcontainers/" + nodeId + "/entrypoint.sh", scriptContent);
    }

    public static void deleteDirectory(String directoryPath)
    {
        File directory = new File(directoryPath);
        deleteDirectory(directory);
    }

    private static void deleteDirectory(File directory)
    {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        if (directory.delete()) {
            logger.info("Deleted: " + directory.getPath());
        }
        else {
            System.err.println("Failed to delete: " + directory.getPath());
        }
    }

    public static void createPropertiesFile(String filePath, Properties properties)
    {
        try {
            File file = new File(filePath);

            File parentDir = file.getParentFile();
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }

            if (file.exists()) {
                throw new IOException("File exists: " + filePath);
            }

            try (FileWriter writer = new FileWriter(file)) {
                for (String key : properties.stringPropertyNames()) {
                    writer.write(key + "=" + properties.getProperty(key) + "\n");
                }
                logger.info(file.getName() + " created successfully with content : " + properties);
            }
        }
        catch (IOException io) {
            io.printStackTrace();
        }
    }

    public static void createScriptFile(String filePath, String scriptContent)
    {
        try {
            File file = new File(filePath);

            File parentDir = file.getParentFile();
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }

            if (file.exists()) {
                throw new IOException("File exists : " + filePath);
            }

            try (OutputStream output = new FileOutputStream(file)) {
                output.write(scriptContent.getBytes());
            }

            file.setExecutable(true);
            logger.info(file.getName() + " created successfully with content :" + scriptContent);
        }
        catch (IOException io) {
            io.printStackTrace();
        }
    }

    public static MaterializedResult toMaterializedResult(String csvData)
    {
        try {
            List<Type> columnTypes = new ArrayList<>();

            // Parse CSV data using OpenCSV
            CSVReader reader = new CSVReader(new StringReader(csvData));
            List<String[]> records = reader.readAll();

            // Collect all rows as lists of strings
            ImmutableList.Builder<List<String>> allRowsBuilder = ImmutableList.builder();
            for (String[] record : records) {
                allRowsBuilder.add(ImmutableList.copyOf(record));
            }
            ImmutableList<List<String>> allRows = allRowsBuilder.build();

            // Infer column types based on the maximum columns found
            int maxColumns = allRows.stream().mapToInt(List::size).max().orElse(0);
            for (int i = 0; i < maxColumns; i++) {
                final int columnIndex = i;
                columnTypes.add(inferType(allRows.stream()
                        .map(row -> columnIndex < row.size() ? row.get(columnIndex) : "")
                        .collect(Collectors.toList())));
            }

            // Convert all rows to MaterializedRow
            ImmutableList.Builder<MaterializedRow> rowsBuilder = ImmutableList.builder();
            for (List<String> columns : allRows) {
                ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
                for (int i = 0; i < columnTypes.size(); i++) {
                    valuesBuilder.add(i < columns.size() ? convertToType(columns.get(i), columnTypes.get(i)) : null);
                }
                rowsBuilder.add(new MaterializedRow(5, valuesBuilder.build()));
            }

            ImmutableList<MaterializedRow> materializedRows = rowsBuilder.build();

            // Create and return the MaterializedResult
            return new MaterializedResult(materializedRows, columnTypes);
        }
        catch (IOException e) {
            fail("Failed to parse CSV data to MaterializedResult with message : ", e);
        }
        return null;
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
            fail("Unsupported type: " + type);
            return null;
        }
    }
}

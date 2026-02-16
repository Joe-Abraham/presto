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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestIcebergV3
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testCreateV3Table()
    {
        String tableName = "test_create_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            assertQuery("SELECT * FROM " + tableName, "SELECT * WHERE false");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpgradeV2ToV3()
    {
        String tableName = "test_upgrade_v2_to_v3";
        try {
            // Create v2 table
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '2')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);

            // Upgrade to v3
            BaseTable baseTable = (BaseTable) table;
            TableOperations operations = baseTable.operations();
            TableMetadata currentMetadata = operations.current();
            operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(3));

            // Verify the upgrade
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertIntoV3Table()
    {
        String tableName = "test_insert_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'one'), (2, 'two')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'three')", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testDeleteOnV3TableNotSupported()
    {
        String tableName = "test_delete_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3', \"write.delete.mode\" = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);
            assertThatThrownBy(() -> getQueryRunner().execute("DELETE FROM " + tableName + " WHERE id = 1"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateOnV3TableNotSupported()
    {
        String tableName = "test_update_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3', \"write.update.mode\" = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);
            assertThatThrownBy(() -> getQueryRunner().execute("UPDATE " + tableName + " SET value = 'updated' WHERE id = 1"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMergeOnV3TableNotSupported()
    {
        String tableName = "test_merge_v3_table";
        String sourceTable = "test_merge_v3_source";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3', \"write.update.mode\" = 'merge-on-read')");
            assertUpdate("CREATE TABLE " + sourceTable + " (id integer, value varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            assertUpdate("INSERT INTO " + sourceTable + " VALUES (1, 'updated')", 1);
            assertThatThrownBy(() -> getQueryRunner().execute(
                    "MERGE INTO " + tableName + " t USING " + sourceTable + " s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
            dropTable(sourceTable);
        }
    }

    @Test
    public void testOptimizeOnV3TableNotSupported()
    {
        String tableName = "test_optimize_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);
            assertThatThrownBy(() -> getQueryRunner().execute(format("CALL system.rewrite_data_files('%s', '%s')", TEST_SCHEMA, tableName)))
                    .hasMessageContaining("OPTIMIZE is not supported for Iceberg table format version > 2");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSelectFromV3TableAfterInsert()
    {
        String tableName = "test_select_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, name varchar, price decimal(10,2)) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'apple', 1.50), (2, 'banana', 0.75), (3, 'cherry', 2.00)", 3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'apple', 1.50), (2, 'banana', 0.75), (3, 'cherry', 2.00)");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
            assertQuery("SELECT sum(price) FROM " + tableName, "SELECT 4.25");
            assertQuery("SELECT name FROM " + tableName + " WHERE price > 1.00 ORDER BY name", "VALUES ('apple'), ('cherry')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithPartitioning()
    {
        String tableName = "test_v3_partitioned_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar, value integer) " +
                    "WITH (\"format-version\" = '3', partitioning = ARRAY['category'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)", 3);
            assertQuery("SELECT * FROM " + tableName + " WHERE category = 'A' ORDER BY id", "VALUES (1, 'A', 100), (3, 'A', 150)");
            assertQuery("SELECT category, sum(value) FROM " + tableName + " GROUP BY category ORDER BY category", "VALUES ('A', 250), ('B', 200)");
        }
        finally {
            dropTable(tableName);
        }
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    @Test
    public void testV3TableWithComplexTypes()
    {
        String tableName = "test_v3_complex_types";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id integer, " +
                    "name varchar, " +
                    "scores array(integer), " +
                    "metadata map(varchar, varchar), " +
                    "details row(field1 integer, field2 varchar)) " +
                    "WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'test1', ARRAY[10, 20, 30], MAP(ARRAY['key1'], ARRAY['value1']), ROW(100, 'detail1'))", 1);
            assertQuery("SELECT id, name FROM " + tableName, "VALUES (1, 'test1')");
            assertQuery("SELECT id, scores[1] FROM " + tableName, "VALUES (1, 10)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithTimestampTypes()
    {
        String tableName = "test_v3_timestamp_types";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id integer, " +
                    "event_time timestamp, " +
                    "event_time_tz timestamp with time zone) " +
                    "WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, TIMESTAMP '2024-01-15 10:30:45.123456', TIMESTAMP '2024-01-15 10:30:45.123456 UTC')", 1);
            assertQuery("SELECT id FROM " + tableName + " WHERE event_time = TIMESTAMP '2024-01-15 10:30:45.123456'", 
                    "VALUES (1)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithAllPrimitiveTypes()
    {
        String tableName = "test_v3_all_primitives";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "col_boolean boolean, " +
                    "col_integer integer, " +
                    "col_bigint bigint, " +
                    "col_real real, " +
                    "col_double double, " +
                    "col_decimal decimal(10,2), " +
                    "col_varchar varchar, " +
                    "col_varbinary varbinary, " +
                    "col_date date, " +
                    "col_time time, " +
                    "col_timestamp timestamp) " +
                    "WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            assertUpdate("INSERT INTO " + tableName + " VALUES (" +
                    "true, 42, 9223372036854775807, REAL '3.14', 2.718, DECIMAL '123.45', " +
                    "'test', X'DEADBEEF', DATE '2024-01-15', TIME '14:30:00', TIMESTAMP '2024-01-15 14:30:00')", 1);
            assertQuery("SELECT col_boolean, col_integer FROM " + tableName, "VALUES (true, 42)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithNullValues()
    {
        String tableName = "test_v3_nulls";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id integer, " +
                    "nullable_int integer, " +
                    "nullable_varchar varchar, " +
                    "nullable_array array(integer)) " +
                    "WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, NULL, NULL, NULL)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 100, 'value', ARRAY[1, 2])", 1);
            assertQuery("SELECT id FROM " + tableName + " WHERE nullable_int IS NULL", "VALUES (1)");
            assertQuery("SELECT id FROM " + tableName + " WHERE nullable_int IS NOT NULL", "VALUES (2)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithNestedComplexTypes()
    {
        String tableName = "test_v3_nested_complex";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id integer, " +
                    "nested_array array(array(integer)), " +
                    "nested_map map(varchar, array(integer)), " +
                    "nested_row row(field1 varchar, field2 array(integer))) " +
                    "WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            assertUpdate("INSERT INTO " + tableName + " VALUES (" +
                    "1, " +
                    "ARRAY[ARRAY[1, 2], ARRAY[3, 4]], " +
                    "MAP(ARRAY['key1'], ARRAY[ARRAY[10, 20]]), " +
                    "ROW('text', ARRAY[100, 200]))", 1);
            assertQuery("SELECT id FROM " + tableName, "VALUES (1)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithPartitionedComplexTypes()
    {
        String tableName = "test_v3_partitioned_complex";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (" +
                    "id integer, " +
                    "category varchar, " +
                    "data array(integer), " +
                    "created_date date) " +
                    "WITH (\"format-version\" = '3', partitioning = ARRAY['category'])");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'A', ARRAY[1, 2, 3], DATE '2024-01-01'), " +
                    "(2, 'B', ARRAY[4, 5, 6], DATE '2024-01-02'), " +
                    "(3, 'A', ARRAY[7, 8, 9], DATE '2024-01-03')", 3);
            assertQuery("SELECT id FROM " + tableName + " WHERE category = 'A' ORDER BY id", 
                    "VALUES (1), (3)");
        }
        finally {
            dropTable(tableName);
        }
    }
}

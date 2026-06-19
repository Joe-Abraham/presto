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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestIcebergAlterColumnNotNull
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testSetNotNull()
    {
        String tableName = "alter_column_not_null_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (c1 BIGINT, c2 BIGINT, c3 VARCHAR, c4 BIGINT)", tableName));

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'NO'), ('c3', 'YES'), ('c4', 'YES')");

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c4 SET NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'NO'), ('c3', 'YES'), ('c4', 'NO')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testNotNullEnforcedOnIcebergWrite()
            throws Exception
    {
        // Enforcement via the Iceberg Java write API (bypasses the Presto table-writer operator).
        // Parquet maps Iceberg required fields to Parquet REQUIRED, causing the writer to throw
        // when a null value is supplied for such a field.
        String tableName = "alter_column_not_null_insert_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (c1 BIGINT, c2 BIGINT, c3 VARCHAR)", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));

            Table icebergTable = loadTable(schema, tableName);
            assertTrue(icebergTable.schema().findField("c2").isRequired(), "c2 should be required (NOT NULL) in Iceberg schema");

            GenericRecord nullRecord = GenericRecord.create(icebergTable.schema());
            nullRecord.setField("c1", 1L);
            nullRecord.setField("c2", null);
            nullRecord.setField("c3", "a");
            assertThatThrownBy(() -> writeRecord(icebergTable, nullRecord))
                    .isInstanceOf(NullPointerException.class);

            GenericRecord validRecord = GenericRecord.create(icebergTable.schema());
            validRecord.setField("c1", 1L);
            validRecord.setField("c2", 2L);
            validRecord.setField("c3", "a");
            writeRecord(icebergTable, validRecord);

            GenericRecord nullableRecord = GenericRecord.create(icebergTable.schema());
            nullableRecord.setField("c1", 2L);
            nullableRecord.setField("c2", 3L);
            nullableRecord.setField("c3", null);
            writeRecord(icebergTable, nullableRecord);

            assertQuery(format("SELECT count(*) FROM %s", tableName), "VALUES 2");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSetNotNullWithExistingNullData()
            throws Exception
    {
        // Iceberg SET NOT NULL is metadata-only: existing rows with NULLs remain readable.
        // Future writes are enforced via the Iceberg Java write API.
        String tableName = "alter_not_null_existing_nulls_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (c1 BIGINT, c2 BIGINT)", tableName));
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL)", tableName), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));

            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'NO')");

            assertQuery(format("SELECT c1, c2 FROM %s", tableName), "VALUES (1, NULL)");

            Table icebergTable = loadTable(schema, tableName);
            assertTrue(icebergTable.schema().findField("c2").isRequired(), "c2 should be required (NOT NULL) in Iceberg schema");

            GenericRecord nullRecord = GenericRecord.create(icebergTable.schema());
            nullRecord.setField("c1", 2L);
            nullRecord.setField("c2", null);
            assertThatThrownBy(() -> writeRecord(icebergTable, nullRecord))
                    .isInstanceOf(NullPointerException.class);

            GenericRecord validRecord = GenericRecord.create(icebergTable.schema());
            validRecord.setField("c1", 2L);
            validRecord.setField("c2", 3L);
            writeRecord(icebergTable, validRecord);

            assertQuery(format("SELECT count(*) FROM %s", tableName), "VALUES 2");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSetNotNullOnStructColumn()
    {
        // SET NOT NULL applies to the top-level struct column, not its nested fields.
        String tableName = "alter_column_not_null_struct_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, payload ROW(x BIGINT, y VARCHAR))", tableName));

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN payload SET NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('id', 'YES'), ('payload', 'NO')");

            Table icebergTable = loadTable(schema, tableName);
            assertTrue(icebergTable.schema().findField("payload").isRequired(), "payload should be required (NOT NULL) in Iceberg schema");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testAlterColumnNotNullOnPartitionColumn()
    {
        String tableName = "alter_column_not_null_partition_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, region VARCHAR) WITH (partitioning = ARRAY['region'])", tableName));

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN region SET NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('id', 'YES'), ('region', 'NO')");

            // NOT NULL enforcement during writes is handled by the Java-side TableWriterOperator
            // and is not enforced in the native (Velox) execution path; enforcement is covered by
            // IcebergDistributedSmokeTestBase.testAlterColumnNotNullOnPartitionColumn.
            assertUpdate(format("INSERT INTO %s VALUES (1, 'us-east')", tableName), 1);
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    private Table loadTable(String schema, String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(schema, tableName));
    }

    private Map<String, String> getProperties()
    {
        return ImmutableMap.of("warehouse", getCatalogDirectory().toURI().toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = IcebergQueryRunner.getIcebergDataDirectoryPath(
                dataDirectory, CatalogType.HADOOP.name(),
                new IcebergConfig().getFileFormat(), true);
        return catalogDirectory.toFile();
    }

    private void writeRecord(Table table, Record record)
            throws Exception
    {
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/" + filename);
        Configuration conf = new Configuration();

        DataWriter<Record> writer = Parquet.writeData(HadoopOutputFile.fromPath(filePath, conf))
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .build();
        try {
            writer.write(record);
        }
        finally {
            writer.close();
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }
}

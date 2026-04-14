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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPrestoNativeIcebergGeneralQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected void createTables()
    {
        createTestTables();
    }

    private void createTestTables()
    {
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());

        javaQueryRunner.execute("DROP TABLE IF EXISTS test_hidden_columns");
        javaQueryRunner.execute("CREATE TABLE test_hidden_columns AS SELECT * FROM tpch.tiny.region WHERE regionkey=0");
        javaQueryRunner.execute("INSERT INTO test_hidden_columns SELECT * FROM tpch.tiny.region WHERE regionkey=1");

        javaQueryRunner.execute("DROP TABLE IF EXISTS ice_table_partitioned");
        javaQueryRunner.execute("CREATE TABLE ice_table_partitioned(c1 INT, ds DATE) WITH (partitioning = ARRAY['ds'])");
        javaQueryRunner.execute("INSERT INTO ice_table_partitioned VALUES(1, date'2022-04-09'), (2, date'2022-03-18'), (3, date'1993-01-01')");

        javaQueryRunner.execute("DROP TABLE IF EXISTS ice_table");
        javaQueryRunner.execute("CREATE TABLE ice_table(c1 INT, ds DATE)");
        javaQueryRunner.execute("INSERT INTO ice_table VALUES(1, date'2022-04-09'), (2, date'2022-03-18'), (3, date'1993-01-01')");

        javaQueryRunner.execute("DROP TABLE IF EXISTS test_analyze");
        javaQueryRunner.execute("CREATE TABLE test_analyze(i int)");
        javaQueryRunner.execute("INSERT INTO test_analyze VALUES 1, 2, 3, 4, 5");

        javaQueryRunner.execute("DROP TABLE IF EXISTS test_row_lineage_hidden");
        javaQueryRunner.execute("CREATE TABLE test_row_lineage_hidden AS SELECT * FROM tpch.tiny.region WHERE regionkey=0");
        javaQueryRunner.execute("INSERT INTO test_row_lineage_hidden SELECT * FROM tpch.tiny.region WHERE regionkey=1");

        javaQueryRunner.execute("DROP TABLE IF EXISTS test_row_lineage_v3");
        javaQueryRunner.execute("CREATE TABLE test_row_lineage_v3 WITH (\"format-version\" = '3') AS SELECT * FROM tpch.tiny.region WHERE regionkey=0");
        javaQueryRunner.execute("INSERT INTO test_row_lineage_v3 SELECT * FROM tpch.tiny.region WHERE regionkey=1");
    }

    @Test
    public void testPathHiddenColumn()
    {
        assertQuery("SELECT \"$path\", * FROM test_hidden_columns");

        // Fetch one of the file paths and use it in a filter
        String filePath = (String) computeActual("SELECT \"$path\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * from test_hidden_columns WHERE \"$path\"='%s'", filePath));

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", filePath))
                        .getOnlyValue(),
                1L);

        // Filter for $path that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", "non-existent-path"))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testDataSequenceNumberHiddenColumn()
    {
        assertQuery("SELECT \"$data_sequence_number\", * FROM test_hidden_columns");

        // Fetch one of the data sequence numbers and use it in a filter
        Long dataSequenceNumber = (Long) computeActual("SELECT \"$data_sequence_number\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber));

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber))
                        .getOnlyValue(),
                1L);

        // Filter for $data_sequence_number that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", 1000))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testDateQueries()
    {
        assertQuery("SELECT * FROM ice_table_partitioned WHERE ds >= date'1994-01-01'", "VALUES (1, date'2022-04-09'), (2, date'2022-03-18')");
        assertQuery("SELECT * FROM ice_table WHERE ds = date'2022-04-09'", "VALUES (1, date'2022-04-09')");
    }

    @Test
    public void testAnalyze()
    {
        assertUpdate(getSession(), "ANALYZE test_analyze", 5);
    }

    @Test
    public void testRowLineageHiddenColumns()
    {
        // For non-V3 tables (format-version = 2, the default), _row_id and _last_updated_sequence_number return null
        assertEquals(computeActual("SELECT \"_row_id\", * FROM test_row_lineage_hidden").getRowCount(), 2);
        assertQuery("SELECT \"_row_id\" FROM test_row_lineage_hidden", "VALUES NULL, NULL");
        assertQuery("SELECT \"_last_updated_sequence_number\" FROM test_row_lineage_hidden", "VALUES NULL, NULL");

        // For V3 tables, _row_id and _last_updated_sequence_number must have actual values
        String v3Table = "test_row_lineage_v3";

        // Both rows must have non-null row lineage values
        assertEquals(computeActual("SELECT \"_row_id\", * FROM " + v3Table).getRowCount(), 2);
        assertEquals(computeActual("SELECT \"_row_id\" FROM " + v3Table + " WHERE \"_row_id\" IS NULL").getRowCount(), 0);
        assertEquals(computeActual("SELECT \"_last_updated_sequence_number\" FROM " + v3Table + " WHERE \"_last_updated_sequence_number\" IS NULL").getRowCount(), 0);

        // _row_id must be unique across all rows in the table
        long distinctRowIds = (Long) computeActual("SELECT count(DISTINCT \"_row_id\") FROM " + v3Table).getOnlyValue();
        assertEquals(distinctRowIds, 2L);

        // _last_updated_sequence_number must differ between the two commits
        long distinctSeqNums = (Long) computeActual("SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + v3Table).getOnlyValue();
        assertEquals(distinctSeqNums, 2L);

        // Rows from the first commit have a smaller sequence number than rows from the second commit
        Long seqForFirst = (Long) computeActual("SELECT \"_last_updated_sequence_number\" FROM " + v3Table + " WHERE regionkey=0").getOnlyValue();
        Long seqForSecond = (Long) computeActual("SELECT \"_last_updated_sequence_number\" FROM " + v3Table + " WHERE regionkey=1").getOnlyValue();
        assertNotNull(seqForFirst);
        assertNotNull(seqForSecond);
        assertTrue(seqForFirst < seqForSecond, "_last_updated_sequence_number should be smaller for earlier commits");

        // Row IDs must differ between the two rows (they are unique)
        Long rowIdForFirst = (Long) computeActual("SELECT \"_row_id\" FROM " + v3Table + " WHERE regionkey=0").getOnlyValue();
        Long rowIdForSecond = (Long) computeActual("SELECT \"_row_id\" FROM " + v3Table + " WHERE regionkey=1").getOnlyValue();
        assertNotNull(rowIdForFirst);
        assertNotNull(rowIdForSecond);
        assertTrue(!rowIdForFirst.equals(rowIdForSecond), "_row_id should be unique per row");
    }
}

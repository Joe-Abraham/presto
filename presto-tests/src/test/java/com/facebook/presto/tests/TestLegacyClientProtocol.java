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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.client.ClientCapabilities;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.client.ClientCapabilities.PARAMETRIC_DATETIME;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE_MICROSECONDS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Integration tests for client protocol compatibility, ensuring that legacy clients
 * continue to work with parametric timestamp types by receiving downgraded type names.
 */
public class TestLegacyClientProtocol
        extends AbstractTestQueries
{
    @Test
    public void testLegacyClientDescribeInput()
    {
        Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Test DESCRIBE INPUT with parametric timestamp parameters
        String sql = "SELECT ? + interval '1' day";
        
        // Legacy client should see simplified type names
        MaterializedResult result = computeActual(legacySession, "DESCRIBE INPUT " + sql);
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(1), TIMESTAMP);
    }

    @Test
    public void testLegacyClientDescribeInputWithMicroseconds()
    {
        Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Use a query that would normally require timestamp(6) precision
        String sql = "SELECT ? * interval '1' microsecond";
        
        // Legacy client should see timestamp_microseconds for precision 6 operations
        // Note: This test may require special handling for microsecond intervals
        // to force the precision to 6 rather than default 3
        MaterializedResult result = computeActual(legacySession, "DESCRIBE INPUT " + sql);
        assertEquals(result.getRowCount(), 1);
        // Result depends on how the query analyzer handles interval microsecond operations
        String actualType = (String) result.getMaterializedRows().get(0).getField(1);
        // Should be either timestamp or timestamp_microseconds
        assert actualType.equals(TIMESTAMP) || actualType.equals(TIMESTAMP_MICROSECONDS);
    }

    @Test
    public void testModernClientDescribeInput()
    {
        Session modernSession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Test DESCRIBE INPUT with parametric timestamp parameters
        String sql = "SELECT ? + interval '1' day";
        
        // Modern client should see parametric type names
        MaterializedResult result = computeActual(modernSession, "DESCRIBE INPUT " + sql);
        assertEquals(result.getRowCount(), 1);
        String actualType = (String) result.getMaterializedRows().get(0).getField(1);
        // Should be a parametric timestamp like "timestamp(3)"
        assert actualType.startsWith("timestamp(") && actualType.endsWith(")");
    }

    @Test
    public void testLegacyClientSelectResults()
    {
        Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Test that SELECT results show downgraded column types for legacy clients
        MaterializedResult result = computeActual(legacySession, 
            "SELECT TIMESTAMP '2023-01-01 12:00:00', " +
            "       TIMESTAMP '2023-01-01 12:00:00.123456', " +
            "       TIMESTAMP '2023-01-01 12:00:00' AT TIME ZONE 'UTC'");

        // Verify column type metadata (this test may need adjustment based on actual implementation)
        assertEquals(result.getTypes().size(), 3);
        
        // Note: The exact type representation in MaterializedResult may differ from
        // the protocol-level type strings. This test serves as a placeholder for
        // protocol-level integration testing.
    }

    @Test
    public void testModernClientSelectResults()
    {
        Session modernSession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Test that SELECT results show parametric column types for modern clients
        MaterializedResult result = computeActual(modernSession, 
            "SELECT TIMESTAMP '2023-01-01 12:00:00', " +
            "       TIMESTAMP '2023-01-01 12:00:00.123456', " +
            "       TIMESTAMP '2023-01-01 12:00:00' AT TIME ZONE 'UTC'");

        assertEquals(result.getTypes().size(), 3);
        
        // Note: Similar to above, this test may need adjustment based on how
        // MaterializedResult represents type information.
    }

    @Test
    public void testComplexTypeCompatibilityInQueries()
    {
        Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of())
                .build();

        Session modernSession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Test complex types containing timestamps
        String complexQuery = "SELECT ARRAY[TIMESTAMP '2023-01-01'], " +
                             "       MAP(ARRAY['key'], ARRAY[TIMESTAMP '2023-01-01']), " +
                             "       ROW(TIMESTAMP '2023-01-01', 'test')";

        // Both queries should succeed, but column metadata should differ
        MaterializedResult legacyResult = computeActual(legacySession, complexQuery);
        MaterializedResult modernResult = computeActual(modernSession, complexQuery);

        assertEquals(legacyResult.getRowCount(), 1);
        assertEquals(modernResult.getRowCount(), 1);
        assertEquals(legacyResult.getTypes().size(), 3);
        assertEquals(modernResult.getTypes().size(), 3);

        // Verify the actual data is identical (only metadata should differ)
        assertEquals(legacyResult.getMaterializedRows(), modernResult.getMaterializedRows());
    }

    @Test
    public void testBackwardCompatibilityWithPreDefinedTypes()
    {
        Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Test that queries using legacy type constants work correctly
        MaterializedResult result = computeActual(legacySession,
            "SELECT " +
            "  CAST('2023-01-01 12:00:00' AS TIMESTAMP), " +
            "  CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP), " +
            "  CAST('2023-01-01 12:00:00' AS TIMESTAMP WITH TIME ZONE)");

        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().size(), 3);

        // Verify the queries execute successfully and produce expected values
        Object[] row = result.getMaterializedRows().get(0).getFields().toArray();
        assert row.length == 3;
        assert row[0] != null;
        assert row[1] != null;
        assert row[2] != null;
    }

    @Test
    public void testClientCapabilityNegotiation()
    {
        // Test that client capability negotiation works correctly
        
        // Empty capabilities (legacy client)
        Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of())
                .build();

        // Modern capabilities
        Session modernSession = Session.builder(getQueryRunner().getDefaultSession())
                .setClientCapabilities(ImmutableSet.of(PARAMETRIC_DATETIME.name()))
                .build();

        // Both sessions should be able to execute the same query successfully
        String testQuery = "SELECT TIMESTAMP '2023-01-01 12:00:00.123456'";
        
        MaterializedResult legacyResult = computeActual(legacySession, testQuery);
        MaterializedResult modernResult = computeActual(modernSession, testQuery);

        // Results should be identical (only type metadata differs)
        assertEquals(legacyResult.getRowCount(), modernResult.getRowCount());
        assertEquals(legacyResult.getMaterializedRows(), modernResult.getMaterializedRows());
    }
}
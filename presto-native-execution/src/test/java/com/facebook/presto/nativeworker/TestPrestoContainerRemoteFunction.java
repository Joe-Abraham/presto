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

public class TestPrestoContainerRemoteFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunnerWithFunctionServer createQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunnerWithFunctionServer();
    }

    @Test
    public void testRemoteAbsNegative10()
    {
        assertEquals(
                computeActual("select remote.default.abs(-10)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "10");
    }

    @Test
    public void testRemoteAbsNegative1230()
    {
        assertEquals(
                computeActual("select remote.default.abs(-1230)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "1230");
    }

    @Test
    public void testRemoteDayInterval()
    {
        assertEquals(
                computeActual("select remote.default.day(interval '2' day)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
    }

    @Test
    public void testRemoteVarbinaryLength()
    {
        assertEquals(
                computeActual("select remote.default.length(CAST('AB' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "2");
    }

    @Test
    public void testRemoteFloor()
    {
        assertEquals(
                computeActual("select remote.default.floor(100000.99)")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "100000.0");
    }

    @Test
    public void testRemoteToBase32Literal()
    {
        assertEquals(
                computeActual("select remote.default.to_base32(CAST('abc' AS VARBINARY))")
                        .getMaterializedRows().get(0).getField(0).toString(),
                "MFRGG===");
    }

    @Test
    public void testRemoteToBase32OnOrders()
    {
        MaterializedResult totalOrdersResult = computeActual("select remote.default.to_base32(CAST(o_comment AS VARBINARY)) from orders");
        List<MaterializedRow> totalOrdersRows = totalOrdersResult.getMaterializedRows();
        assertFalse(totalOrdersRows.isEmpty(), "Expected rows from remote.default.to_base32 on orders table");
    }
}

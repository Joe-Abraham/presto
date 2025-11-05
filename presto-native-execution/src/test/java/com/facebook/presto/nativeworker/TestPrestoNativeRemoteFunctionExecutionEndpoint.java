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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.REMOTE_FUNCTION_CATALOG_NAME;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.setupJsonFunctionNamespaceManager;

/**
 * Test to verify that the executionEndpoint field from function metadata
 * is properly fed through to VeloxRemoteFunctionMetadata.location
 */
@Test(groups = "remote-function")
public class TestPrestoNativeRemoteFunctionExecutionEndpoint
        extends AbstractTestNativeRemoteFunctions
{
    private static final String EXECUTION_ENDPOINT_JSON_SIGNATURES = "remote_function_execution_endpoint.json";

    @Override
    protected void postInitQueryRunners()
    {
        // Install json function registration namespace manager with a JSON file that includes executionEndpoint
        setupJsonFunctionNamespaceManager(getQueryRunner(), EXECUTION_ENDPOINT_JSON_SIGNATURES, REMOTE_FUNCTION_CATALOG_NAME);
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setRemoteFunctionServerUds(Optional.of(remoteFunctionServerUds))
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testRemoteFunctionWithInvalidExecutionEndpoint()
    {
        // Test that a function with an invalid executionEndpoint fails with a connection error
        // This proves that the executionEndpoint from JSON is being used (as it tries to connect to the invalid URL)
        assertQueryFails("SELECT remote.schema.ceil_with_invalid_endpoint(linenumber) FROM lineitem LIMIT 1",
                ".*Failed to connect.*http://invalid-endpoint:9999.*");
    }
}

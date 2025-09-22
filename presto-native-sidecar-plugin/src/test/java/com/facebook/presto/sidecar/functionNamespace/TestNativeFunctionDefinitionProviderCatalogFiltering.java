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
package com.facebook.presto.sidecar.functionNamespace;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.TestingSessionPropertyManager;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestNativeFunctionDefinitionProviderCatalogFiltering
{
    private JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> codec;
    private NodeManager nodeManager;

    @BeforeTest
    public void setUp()
    {
        codec = jsonCodec(new com.fasterxml.jackson.core.type.TypeReference<Map<String, List<JsonBasedUdfFunctionMetadata>>>() {});
        nodeManager = mock(NodeManager.class);
        
        // Mock sidecar node
        com.facebook.presto.spi.Node sidecarNode = mock(com.facebook.presto.spi.Node.class);
        when(sidecarNode.getHttpUri()).thenReturn(URI.create("http://localhost:8080"));
        when(nodeManager.getSidecarNode()).thenReturn(sidecarNode);
    }

    @Test
    public void testCatalogFilteredEndpoint()
    {
        // Mock response for catalog-filtered endpoint
        Map<String, List<JsonBasedUdfFunctionMetadata>> catalogFilteredResponse = ImmutableMap.of(
                "test_function", ImmutableList.of()
        );
        String catalogFilteredJson = codec.toJson(catalogFilteredResponse);

        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            if (request.getUri().getPath().equals("/v1/functions/hive")) {
                return new TestingResponse(com.facebook.airlift.http.client.HttpStatus.OK, ImmutableMap.of(), catalogFilteredJson.getBytes());
            } else {
                // Default response for all functions
                Map<String, List<JsonBasedUdfFunctionMetadata>> allResponse = ImmutableMap.of(
                        "test_function", ImmutableList.of(),
                        "another_function", ImmutableList.of()
                );
                return new TestingResponse(com.facebook.airlift.http.client.HttpStatus.OK, ImmutableMap.of(), codec.toJson(allResponse).getBytes());
            }
        });

        // Test with catalog filtering enabled
        NativeFunctionNamespaceManagerConfig configWithCatalog = new NativeFunctionNamespaceManagerConfig()
                .setCatalogName("hive")
                .setSidecarNumRetries(1)
                .setSidecarRetryDelay(new com.facebook.airlift.units.Duration(1, java.util.concurrent.TimeUnit.SECONDS));

        NativeFunctionDefinitionProvider providerWithCatalog = new NativeFunctionDefinitionProvider(
                httpClient, codec, configWithCatalog);

        UdfFunctionSignatureMap resultWithCatalog = providerWithCatalog.getUdfDefinition(nodeManager);
        assertEquals(resultWithCatalog.getUDFSignatureMap().size(), 1);
        assertTrue(resultWithCatalog.getUDFSignatureMap().containsKey("test_function"));

        // Test with no catalog filtering (empty catalog name)
        NativeFunctionNamespaceManagerConfig configNoCatalog = new NativeFunctionNamespaceManagerConfig()
                .setCatalogName("")
                .setSidecarNumRetries(1)
                .setSidecarRetryDelay(new com.facebook.airlift.units.Duration(1, java.util.concurrent.TimeUnit.SECONDS));

        NativeFunctionDefinitionProvider providerNoCatalog = new NativeFunctionDefinitionProvider(
                httpClient, codec, configNoCatalog);

        UdfFunctionSignatureMap resultNoCatalog = providerNoCatalog.getUdfDefinition(nodeManager);
        assertEquals(resultNoCatalog.getUDFSignatureMap().size(), 2);
        assertTrue(resultNoCatalog.getUDFSignatureMap().containsKey("test_function"));
        assertTrue(resultNoCatalog.getUDFSignatureMap().containsKey("another_function"));
    }
}
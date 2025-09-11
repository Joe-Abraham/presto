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

import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestMultipleCatalogFunctionNamespaces
{
    private static final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> FUNCTION_MAP_CODEC =
            mapJsonCodec(String.class, JsonCodec.listJsonCodec(JsonBasedUdfFunctionMetadata.class));

    private InMemoryNodeManager nodeManager;
    private TestingHttpClient httpClient;

    @BeforeMethod
    public void setUp()
    {
        nodeManager = new InMemoryNodeManager();
        httpClient = new TestingHttpClient();
    }

    @Test
    public void testMultipleCatalogEndpoints()
    {
        // Simulate a sidecar node
        InternalNode sidecarNode = new InternalNode("sidecar-1", URI.create("http://localhost:8080"), new NodeVersion("1"), false, false, false, true);
        nodeManager.addNode(new ConnectorId("sidecar"), sidecarNode);

        // Mock responses for different catalog endpoints
        Map<String, List<JsonBasedUdfFunctionMetadata>> hiveFunctions = ImmutableMap.of(
                "initcap", List.of(createMockHiveFunctionMetadata("initcap", "hive", "default")));

        Map<String, List<JsonBasedUdfFunctionMetadata>> builtinFunctions = ImmutableMap.of(
                "abs", List.of(createMockBuiltinFunctionMetadata("abs", "presto", "default")));

        // Set up HTTP client responses for different catalog endpoints
        httpClient.expectCall()
                .method("GET")
                .url("http://localhost:8080/v1/functions/hive")
                .andReturn(new TestingResponse(OK, ImmutableMap.of(), FUNCTION_MAP_CODEC.toJsonBytes(hiveFunctions)));

        httpClient.expectCall()
                .method("GET")
                .url("http://localhost:8080/v1/functions/presto.default")
                .andReturn(new TestingResponse(OK, ImmutableMap.of(), FUNCTION_MAP_CODEC.toJsonBytes(builtinFunctions)));

        // Test Hive catalog functions
        NativeFunctionNamespaceManagerConfig hiveConfig = new NativeFunctionNamespaceManagerConfig()
                .setCatalog("hive");

        NativeFunctionDefinitionProvider hiveProvider = new NativeFunctionDefinitionProvider(
                httpClient, FUNCTION_MAP_CODEC, hiveConfig);

        UdfFunctionSignatureMap hiveSignatures = hiveProvider.getUdfDefinition(nodeManager);

        assertNotNull(hiveSignatures);
        assertTrue(hiveSignatures.getUDFSignatureMap().containsKey("initcap"));
        assertEquals(hiveSignatures.getUDFSignatureMap().get("initcap").size(), 1);

        // Test built-in catalog functions
        NativeFunctionNamespaceManagerConfig builtinConfig = new NativeFunctionNamespaceManagerConfig()
                .setCatalog("presto.default");

        NativeFunctionDefinitionProvider builtinProvider = new NativeFunctionDefinitionProvider(
                httpClient, FUNCTION_MAP_CODEC, builtinConfig);

        UdfFunctionSignatureMap builtinSignatures = builtinProvider.getUdfDefinition(nodeManager);

        assertNotNull(builtinSignatures);
        assertTrue(builtinSignatures.getUDFSignatureMap().containsKey("abs"));
        assertEquals(builtinSignatures.getUDFSignatureMap().get("abs").size(), 1);
    }

    @Test
    public void testEmptyCatalogFetchesAllFunctions()
    {
        // Test that empty catalog fetches from /v1/functions endpoint (all functions)
        InternalNode sidecarNode = new InternalNode("sidecar-1", URI.create("http://localhost:8080"), new NodeVersion("1"), false, false, false, true);
        nodeManager.addNode(new ConnectorId("sidecar"), sidecarNode);

        Map<String, List<JsonBasedUdfFunctionMetadata>> allFunctions = ImmutableMap.of(
                "initcap", List.of(createMockHiveFunctionMetadata("initcap", "hive", "default")),
                "abs", List.of(createMockBuiltinFunctionMetadata("abs", "presto", "default")));

        httpClient.expectCall()
                .method("GET")
                .url("http://localhost:8080/v1/functions")
                .andReturn(new TestingResponse(OK, ImmutableMap.of(), FUNCTION_MAP_CODEC.toJsonBytes(allFunctions)));

        NativeFunctionNamespaceManagerConfig config = new NativeFunctionNamespaceManagerConfig()
                .setCatalog(""); // Empty catalog

        NativeFunctionDefinitionProvider provider = new NativeFunctionDefinitionProvider(
                httpClient, FUNCTION_MAP_CODEC, config);

        UdfFunctionSignatureMap signatures = provider.getUdfDefinition(nodeManager);

        assertNotNull(signatures);
        assertTrue(signatures.getUDFSignatureMap().containsKey("initcap"));
        assertTrue(signatures.getUDFSignatureMap().containsKey("abs"));
    }

    private JsonBasedUdfFunctionMetadata createMockHiveFunctionMetadata(String name, String catalog, String schema)
    {
        JsonBasedUdfFunctionMetadata metadata = new JsonBasedUdfFunctionMetadata();
        metadata.setDocString(name);
        metadata.setSchema(schema);
        metadata.setOutputType("varchar");
        metadata.setParamTypes(List.of("varchar"));
        return metadata;
    }

    private JsonBasedUdfFunctionMetadata createMockBuiltinFunctionMetadata(String name, String catalog, String schema)
    {
        JsonBasedUdfFunctionMetadata metadata = new JsonBasedUdfFunctionMetadata();
        metadata.setDocString(name);
        metadata.setSchema(schema);
        metadata.setOutputType("bigint");
        metadata.setParamTypes(List.of("bigint"));
        return metadata;
    }
}

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
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestMultipleCatalogFunctionNamespaces
{
    private static final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> FUNCTION_MAP_CODEC =
            mapJsonCodec(String.class, JsonCodec.listJsonCodec(JsonBasedUdfFunctionMetadata.class));

    private static final URI REST_SERVER_URI = URI.create("http://localhost:8080");

    private InMemoryNodeManager nodeManager;
    private TestingHttpClient httpClient;

    @Path("/v1/functions")
    public static class MockFunctionResource
    {
        private final Map<String, List<JsonBasedUdfFunctionMetadata>> hiveFunctions;
        private final Map<String, List<JsonBasedUdfFunctionMetadata>> builtinFunctions;
        private final Map<String, List<JsonBasedUdfFunctionMetadata>> allFunctions;

        public MockFunctionResource()
        {
            this.hiveFunctions = ImmutableMap.of(
                    "initcap", List.of(createMockHiveFunctionMetadata("initcap", "hive", "default")));
            
            this.builtinFunctions = ImmutableMap.of(
                    "abs", List.of(createMockBuiltinFunctionMetadata("abs", "presto", "default")));
                    
            this.allFunctions = ImmutableMap.<String, List<JsonBasedUdfFunctionMetadata>>builder()
                    .putAll(hiveFunctions)
                    .putAll(builtinFunctions)
                    .build();
        }

        @GET
        @Produces(MediaType.APPLICATION_JSON)
        public Map<String, List<JsonBasedUdfFunctionMetadata>> getAllFunctions()
        {
            return allFunctions;
        }

        @GET
        @Path("/{catalog}")
        @Produces(MediaType.APPLICATION_JSON)
        public Map<String, List<JsonBasedUdfFunctionMetadata>> getFunctionsByCatalog(@PathParam("catalog") String catalog)
        {
            if ("hive".equals(catalog)) {
                return hiveFunctions;
            } else if ("presto.default".equals(catalog)) {
                return builtinFunctions;
            }
            return ImmutableMap.of();
        }
        
        private static JsonBasedUdfFunctionMetadata createMockHiveFunctionMetadata(String name, String catalog, String schema)
        {
            return new JsonBasedUdfFunctionMetadata(
                    name + " function description",
                    FunctionKind.SCALAR,
                    parseTypeSignature("varchar"),
                    List.of(parseTypeSignature("varchar")),
                    schema,
                    false,
                    new RoutineCharacteristics(
                            RoutineCharacteristics.Language.CPP, 
                            RoutineCharacteristics.Determinism.DETERMINISTIC, 
                            RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of("1"),
                    Optional.of(emptyList()),
                    Optional.of(emptyList()),
                    Optional.empty());
        }

        private static JsonBasedUdfFunctionMetadata createMockBuiltinFunctionMetadata(String name, String catalog, String schema)
        {
            return new JsonBasedUdfFunctionMetadata(
                    name + " function description",
                    FunctionKind.SCALAR,
                    parseTypeSignature("bigint"),
                    List.of(parseTypeSignature("bigint")),
                    schema,
                    false,
                    new RoutineCharacteristics(
                            RoutineCharacteristics.Language.CPP, 
                            RoutineCharacteristics.Determinism.DETERMINISTIC, 
                            RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of("1"),
                    Optional.of(emptyList()),
                    Optional.of(emptyList()),
                    Optional.empty());
        }
    }

    @BeforeMethod
    public void setUp()
    {
        nodeManager = new InMemoryNodeManager();
        
        MockFunctionResource resource = new MockFunctionResource();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        JaxrsTestingHttpProcessor httpProcessor = new JaxrsTestingHttpProcessor(
                UriBuilder.fromUri(REST_SERVER_URI).path("/").build(),
                resource,
                new JsonMapper(mapper));
        httpClient = new TestingHttpClient(httpProcessor);
    }

    @Test
    public void testMultipleCatalogEndpoints()
    {
        // Simulate a sidecar node
        InternalNode sidecarNode = new InternalNode("sidecar-1", URI.create("http://localhost:8080"), new NodeVersion("1"), false, false, false, true);
        nodeManager.addNode(new ConnectorId("sidecar"), sidecarNode);

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

        NativeFunctionNamespaceManagerConfig config = new NativeFunctionNamespaceManagerConfig()
                .setCatalog(""); // Empty catalog

        NativeFunctionDefinitionProvider provider = new NativeFunctionDefinitionProvider(
                httpClient, FUNCTION_MAP_CODEC, config);

        UdfFunctionSignatureMap signatures = provider.getUdfDefinition(nodeManager);

        assertNotNull(signatures);
        assertTrue(signatures.getUDFSignatureMap().containsKey("initcap"));
        assertTrue(signatures.getUDFSignatureMap().containsKey("abs"));
    }
}

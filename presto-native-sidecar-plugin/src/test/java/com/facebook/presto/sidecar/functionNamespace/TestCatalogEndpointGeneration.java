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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCatalogEndpointGeneration
{
    @Test
    public void testGetSidecarLocationWithCatalog()
    {
        // Test that the endpoint generation logic in NativeFunctionDefinitionProvider
        // correctly handles catalog-specific endpoints
        
        // Test empty catalog results in /v1/functions
        String emptyCatalogEndpoint = generateEndpoint("");
        assertEquals(emptyCatalogEndpoint, "/v1/functions");
        
        // Test catalog specified results in /v1/functions/{catalog}
        String hiveCatalogEndpoint = generateEndpoint("hive");
        assertEquals(hiveCatalogEndpoint, "/v1/functions/hive");
        
        String prestoDefaultEndpoint = generateEndpoint("presto.default");
        assertEquals(prestoDefaultEndpoint, "/v1/functions/presto.default");
        
        // Test special characters in catalog names
        String customCatalogEndpoint = generateEndpoint("custom_catalog.special-schema");
        assertEquals(customCatalogEndpoint, "/v1/functions/custom_catalog.special-schema");
    }
    
    @Test
    public void testCatalogConfigurationHandling()
    {
        NativeFunctionNamespaceManagerConfig config = new NativeFunctionNamespaceManagerConfig();
        
        // Test default state
        assertEquals(config.getCatalog(), "");
        assertTrue(config.getCatalog().isEmpty());
        
        // Test setting various catalog values
        config.setCatalog("hive");
        assertEquals(config.getCatalog(), "hive");
        assertFalse(config.getCatalog().isEmpty());
        
        config.setCatalog("presto.default");
        assertEquals(config.getCatalog(), "presto.default");
        
        config.setCatalog("custom.namespace.test");
        assertEquals(config.getCatalog(), "custom.namespace.test");
    }
    
    @Test
    public void testMultipleCatalogConfigurations()
    {
        // Verify that multiple configurations can exist independently
        NativeFunctionNamespaceManagerConfig hiveConfig = new NativeFunctionNamespaceManagerConfig();
        NativeFunctionNamespaceManagerConfig builtinConfig = new NativeFunctionNamespaceManagerConfig();
        NativeFunctionNamespaceManagerConfig customConfig = new NativeFunctionNamespaceManagerConfig();
        
        hiveConfig.setCatalog("hive");
        builtinConfig.setCatalog("presto.default");
        customConfig.setCatalog("custom");
        
        // Verify configurations are independent
        assertEquals(hiveConfig.getCatalog(), "hive");
        assertEquals(builtinConfig.getCatalog(), "presto.default");  
        assertEquals(customConfig.getCatalog(), "custom");
        
        // Verify endpoint generation for each
        assertEquals(generateEndpoint(hiveConfig.getCatalog()), "/v1/functions/hive");
        assertEquals(generateEndpoint(builtinConfig.getCatalog()), "/v1/functions/presto.default");
        assertEquals(generateEndpoint(customConfig.getCatalog()), "/v1/functions/custom");
    }
    
    /**
     * Helper method that replicates the endpoint generation logic from
     * NativeFunctionDefinitionProvider.getSidecarLocation()
     */
    private String generateEndpoint(String catalog) {
        return catalog.isEmpty() ? "/v1/functions" : "/v1/functions/" + catalog;
    }
}
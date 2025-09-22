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
package com.facebook.presto.sidecar;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;

import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPluginWithCatalog;

public class NativeSidecarPluginQueryRunner
{
    private NativeSidecarPluginQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        // You need to add "--user user" to your CLI for your queries to work.
        Logging.initialize();
        
        // Support catalog filtering via system property
        String catalogName = System.getProperty("sidecar.catalog", "");
        
        Logger log = Logger.get(NativeSidecarPluginQueryRunner.class);
        if (!catalogName.isEmpty()) {
            log.info("Starting Native Sidecar with catalog filtering: %s", catalogName);
        } else {
            log.info("Starting Native Sidecar with no catalog filtering (all functions)");
        }

        // Create tables before launching distributed runner.
        QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(false)
                .build();
        NativeQueryRunnerUtils.createAllTables(javaQueryRunner);
        javaQueryRunner.close();

        // Launch distributed runner.
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setCoordinatorSidecarEnabled(true)
                .build();
        
        // Setup with catalog filtering if specified
        if (!catalogName.isEmpty()) {
            setupNativeSidecarPluginWithCatalog(queryRunner, catalogName);
        } else {
            setupNativeSidecarPlugin(queryRunner);
        }
        
        Thread.sleep(10);
        Logger startupLog = Logger.get(DistributedQueryRunner.class);
        startupLog.info("======== SERVER STARTED ========");
        startupLog.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        
        if (!catalogName.isEmpty()) {
            startupLog.info("Catalog filtering enabled for: %s", catalogName);
            startupLog.info("Use queries like 'SHOW FUNCTIONS' to see catalog-filtered functions");
        }
    }
}

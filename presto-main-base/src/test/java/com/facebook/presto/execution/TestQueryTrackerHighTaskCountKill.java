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
package com.facebook.presto.execution;

import com.facebook.presto.resourcemanager.ClusterQueryTrackerService;
import com.facebook.presto.resourcemanager.ResourceManagerClient;
import com.facebook.presto.resourcemanager.ResourceManagerConfig;
import com.facebook.presto.resourcemanager.TestingClusterQueryTrackerService;
import com.facebook.presto.resourcemanager.TestingResourceManagerClient;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.CLUSTER_HAS_TOO_MANY_RUNNING_TASKS;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestQueryTrackerHighTaskCountKill
{
    @Test
    public void testMultipleQueriesKilledDueToTaskCount()
    {
        QueryManagerConfig config = new QueryManagerConfig()
                .setMaxQueryRunningTaskCount(100)
                .setMaxTotalRunningTaskCountToKillQuery(200);
        ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor();
        try {
            QueryTracker<MockQueryExecution> queryTracker = new QueryTracker<>(config, scheduledExecutorService, Optional.empty());
            MockQueryExecution smallQuery1 = MockQueryExecution.withRunningTaskCount(50);
            MockQueryExecution largeQueryButNotKilled = MockQueryExecution.withRunningTaskCount(101);
            MockQueryExecution largeQueryToBeKilled1 = MockQueryExecution.withRunningTaskCount(200);
            MockQueryExecution largeQueryToBeKilled2 = MockQueryExecution.withRunningTaskCount(250);

            queryTracker.addQuery(smallQuery1);
            queryTracker.addQuery(largeQueryButNotKilled);
            queryTracker.addQuery(largeQueryToBeKilled1);
            queryTracker.addQuery(largeQueryToBeKilled2);

            queryTracker.enforceTaskLimits();

            assertFalse(smallQuery1.getFailureReason().isPresent(), "small query should not be killed");
            assertFalse(
                    largeQueryButNotKilled.getFailureReason().isPresent(),
                    "query exceeds per query limit, but not killed since not heaviest and cluster can get into better state");
            assertTrue(largeQueryToBeKilled1.getFailureReason().isPresent(), "Query should be killed");
            Throwable failureReason1 = largeQueryToBeKilled1.getFailureReason().get();
            assertTrue(failureReason1 instanceof PrestoException);
            assertEquals(((PrestoException) failureReason1).getErrorCode(), CLUSTER_HAS_TOO_MANY_RUNNING_TASKS.toErrorCode());
            assertTrue(largeQueryToBeKilled2.getFailureReason().isPresent(), "Query should be killed");
            Throwable failureReason2 = largeQueryToBeKilled2.getFailureReason().get();
            assertTrue(failureReason2 instanceof PrestoException);
            assertEquals(((PrestoException) failureReason2).getErrorCode(), CLUSTER_HAS_TOO_MANY_RUNNING_TASKS.toErrorCode());
        }
        finally {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Test
    public void testLargeQueryKilledDueToTaskCount_withClusterQueryTracker()
    {
        QueryManagerConfig config = new QueryManagerConfig()
                .setMaxQueryRunningTaskCount(100)
                .setMaxTotalRunningTaskCountToKillQuery(200);
        ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor();
        ResourceManagerClient resourceManagerClient = new TestingResourceManagerClient();
        ClusterQueryTrackerService clusterQueryTrackerService = new TestingClusterQueryTrackerService((addressSelectionContext, headers) -> resourceManagerClient, newSingleThreadScheduledExecutor(), new ResourceManagerConfig(), 201);
        try {
            QueryTracker<MockQueryExecution> queryTracker = new QueryTracker<>(config, scheduledExecutorService, Optional.of(clusterQueryTrackerService));
            MockQueryExecution smallQuery = MockQueryExecution.withRunningTaskCount(50);
            MockQueryExecution largeQueryToBeKilled = MockQueryExecution.withRunningTaskCount(101);

            queryTracker.addQuery(smallQuery);
            queryTracker.addQuery(largeQueryToBeKilled);

            queryTracker.enforceTaskLimits();

            assertFalse(smallQuery.getFailureReason().isPresent(), "small query should not be killed");
            Throwable failureReason = largeQueryToBeKilled.getFailureReason().get();
            assertTrue(failureReason instanceof PrestoException);
            assertEquals(((PrestoException) failureReason).getErrorCode(), CLUSTER_HAS_TOO_MANY_RUNNING_TASKS.toErrorCode());
        }
        finally {
            scheduledExecutorService.shutdownNow();
        }
    }
}

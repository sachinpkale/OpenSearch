/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.Segment;
import org.opensearch.indices.replication.SegmentReplicationIT;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreIT extends OpenSearchIntegTestCase {

    private static final String REPOSITORY_NAME = "test-remore-store-repo";
    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    private Settings remoteStoreIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(super.indexSettings())
            .put("index.refresh_interval", "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    private Settings remoteTranslogIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(numberOfReplicas))
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @BeforeClass
    public static void assumeFeatureFlag() {
        System.setProperty(FeatureFlags.REPLICATION_TYPE, "true");
        System.setProperty(FeatureFlags.REMOTE_STORE, "true");
        assumeTrue("Remote Store Feature flag is enabled", Boolean.parseBoolean(System.getProperty(FeatureFlags.REMOTE_STORE)));
    }

    @Before
    public void setup() {
        Path absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(clusterAdmin().preparePutRepository(REPOSITORY_NAME).setType("fs").setSettings(Settings.builder().put("location", absolutePath)));
    }

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
    }

    public void testRemoteStoreRestoreOnCommit() throws IOException {
        internalCluster().startNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").get();
        flush(INDEX_NAME);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));

        client()
            .admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME), PlainActionFuture.newFuture());

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 2);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("abc", "xyz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);
    }

    public void testRemoteStoreRestoreOnRefresh() throws IOException {
        internalCluster().startNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").get();
        refresh(INDEX_NAME);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));

        client()
            .admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME), PlainActionFuture.newFuture());

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 2);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("abc", "xyz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);
    }

    public void testRemoteTranslogRestore() throws IOException {
        internalCluster().startNodes(3);
        createIndex(INDEX_NAME, remoteTranslogIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").get();
        flush(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("abc", "xyz").get();

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));

        client()
            .admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME), PlainActionFuture.newFuture());

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);

        client().prepareIndex(INDEX_NAME).setId("4").setSource("jkl", "pqr").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 4);
    }

    public void testRemoteStoreFailover() throws Exception {
        internalCluster().startNodes(2);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String primary = primaryNodeName(INDEX_NAME);
        final String replica = replicaNodeName(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        assertHitCount(client(primary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        final String replicaDataPath = Environment.PATH_DATA_SETTING.get(internalCluster().dataPathSettings(replica)).get(0);
        System.out.println("################################################################");
        System.out.println(replicaDataPath);
        System.out.println("################################################################");
        for
        FileSystemUtils.files(Path.of(replicaDataPath))
        // ToDo: Delete data from secondary before stopping primary so that we can test segment download part.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).get(), 1);

        client().prepareIndex(INDEX_NAME).setId("2").setSource("abc", "xyz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).get(), 2);
    }

    protected String replicaNodeName(String indexName) {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index(indexName).shard(0).replicaShards().get(0).currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }
}

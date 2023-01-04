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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreIT extends OpenSearchIntegTestCase {

    private static final String REPOSITORY_NAME = "test-remore-store-repo";
    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings();
    }

    private Settings remoteStoreIndexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put("index.refresh_interval", "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    private Settings remoteTranslogIndexSettings() {
        return Settings.builder()
            .put(remoteStoreIndexSettings())
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
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
        createIndex(INDEX_NAME, remoteStoreIndexSettings());
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").get();
        flush(INDEX_NAME);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName()));
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));

        client()
            .admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME), PlainActionFuture.newFuture());

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 2);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("abc", "xyz").get();
        flush(INDEX_NAME);

        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);
    }

    public void testRemoteStoreRestoreOnRefresh() throws IOException {
        internalCluster().startNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings());
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").get();
        refresh(INDEX_NAME);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName()));
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));

        client()
            .admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME), PlainActionFuture.newFuture());

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 2);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("abc", "xyz").get();
        refresh(INDEX_NAME);

        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);
    }

    public void testRemoteTranslogRestore() throws IOException {
        internalCluster().startNodes(3);
        createIndex(INDEX_NAME, remoteTranslogIndexSettings());
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").get();
        refresh(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("abc", "xyz").get();

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName()));
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));

        client()
            .admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME), PlainActionFuture.newFuture());

        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);

        client().prepareIndex(INDEX_NAME).setId("4").setSource("jkl", "pqr").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 4);
    }

    private String primaryNodeName() {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }
}

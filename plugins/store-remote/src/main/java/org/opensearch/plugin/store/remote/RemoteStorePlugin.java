/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugin.store.remote;

import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.remote.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.remote.OpenSearchDirectoryFactory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class RemoteStorePlugin extends Plugin implements IndexStorePlugin {

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories(Supplier<RepositoriesService> repositoriesService, ThreadPool threadPool) {
        final Map<String, DirectoryFactory> indexStoreFactories = new HashMap<>(1);
        final IndexStorePlugin.DirectoryFactory remoteDirectoryFactory = new RemoteSegmentStoreDirectoryFactory(repositoriesService, threadPool);
        indexStoreFactories.put("remote_fs", new OpenSearchDirectoryFactory(remoteDirectoryFactory, new FsDirectoryFactory()));
        return Collections.unmodifiableMap(indexStoreFactories);
    }
}

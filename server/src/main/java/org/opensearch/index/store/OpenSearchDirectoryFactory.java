/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;

import java.io.IOException;

public class OpenSearchDirectoryFactory  implements IndexStorePlugin.DirectoryFactory {
    IndexStorePlugin.DirectoryFactory remoteDirectoryFactory;
    IndexStorePlugin.DirectoryFactory directoryFactory;

    public OpenSearchDirectoryFactory(IndexStorePlugin.DirectoryFactory remoteDirectoryFactory, IndexStorePlugin.DirectoryFactory directoryFactory) {
        this.remoteDirectoryFactory = remoteDirectoryFactory;
        this.directoryFactory = directoryFactory;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        Directory storageDirectory = remoteDirectoryFactory.newDirectory(indexSettings, shardPath);
        Directory cacheDirectory = directoryFactory.newDirectory(indexSettings, shardPath);
        return new OpenSearchDirectory(cacheDirectory, storageDirectory);
    }
}

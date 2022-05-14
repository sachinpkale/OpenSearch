/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    private final Directory storeDirectory;
    private final Directory remoteDirectory;
    private final Map<String, String> filesUploadedToRemoteStore;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(Directory storeDirectory, Directory remoteDirectory) {
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
        this.filesUploadedToRemoteStore = new ConcurrentHashMap<>();
    }

    @Override
    public void beforeRefresh() throws IOException {
        // ToDo Add implementation
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (didRefresh) {
            // ToDo:
            //   1. Based on a config, run the following logic async
            //   2. Instead of deleting files in sync manner, mark them and delete later in another thread.
            Set<String> localFiles = Arrays.stream(storeDirectory.listAll()).collect(Collectors.toSet());
            for (String file : localFiles) {
                if (!filesUploadedToRemoteStore.containsKey(file)) {
                    try {
                        remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
                        filesUploadedToRemoteStore.put(file, file);
                    } catch (NoSuchFileException e) {
                        logger.info("The file {} does not exist anymore. It can happen in case of temp files", file);
                    }
                }
            }
            Set<String> remoteFilesToBeDeleted = new HashSet<>();
            for (String file : filesUploadedToRemoteStore.keySet()) {
                if (!localFiles.contains(file)) {
                    remoteDirectory.deleteFile(file);
                    remoteFilesToBeDeleted.add(file);
                }
            }
            if (!remoteFilesToBeDeleted.isEmpty()) {
                remoteFilesToBeDeleted.forEach(filesUploadedToRemoteStore::remove);
            }
        }
    }
}

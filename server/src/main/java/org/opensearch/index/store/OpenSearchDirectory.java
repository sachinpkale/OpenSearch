/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.*;
import org.opensearch.index.shard.OpenSearchDirectorySyncManager;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class OpenSearchDirectory extends Directory {

    private Directory cache;
    private Directory storage;

    public OpenSearchDirectory(Directory cache, Directory storage) throws IOException {
        this.cache = cache;
        this.storage = storage;
        OpenSearchDirectorySyncManager.syncStorageFilesToCache(this);
    }

    @Override
    public String[] listAll() throws IOException {
        return cache.listAll();
    }

    @Override
    public void deleteFile(String s) throws IOException {
        cache.deleteFile(s);
    }

    @Override
    public long fileLength(String s) throws IOException {
        return cache.fileLength(s);
    }

    @Override
    public IndexOutput createOutput(String s, IOContext ioContext) throws IOException {
        return cache.createOutput(s, ioContext);
    }

    @Override
    public IndexOutput createTempOutput(String s, String s1, IOContext ioContext) throws IOException {
        return cache.createTempOutput(s, s1, ioContext);
    }

    @Override
    public void sync(Collection<String> collection) throws IOException {
        if (collection == null) {
            OpenSearchDirectorySyncManager.syncStorageFilesToCache(this);
        } else {
            cache.sync(collection);
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        cache.syncMetaData();
    }

    @Override
    public void rename(String s, String s1) throws IOException {
        cache.rename(s, s1);
    }

    @Override
    public IndexInput openInput(String s, IOContext ioContext) throws IOException {
        return cache.openInput(s, ioContext);
    }

    @Override
    public Lock obtainLock(String s) throws IOException {
        return cache.obtainLock(s);
    }

    @Override
    public void close() throws IOException {
        cache.close();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return cache.getPendingDeletions();
    }

    public Directory getCache() {
        return cache;
    }

    public Directory getStorage() {
        return storage;
    }
}

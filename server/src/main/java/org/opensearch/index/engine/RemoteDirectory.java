/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.IOUtils;
import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RemoteDirectory extends Directory {

    private final BlobContainer blobContainer;

    public RemoteDirectory(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    @Override
    public String[] listAll() throws IOException {
        return blobContainer.listBlobs().keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        blobContainer.deleteBlobsIgnoringIfNotExists(Collections.singletonList(name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new RemoteIndexOutput(name, context, blobContainer);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new RemoteIndexInput(name, blobContainer.readBlob(name));
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        blobContainer.delete();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return new HashSet<>();
    }
}

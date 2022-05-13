/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Set;

public class CompositeDirectory extends FilterDirectory {
    private final Directory directory1;
    private final Directory directory2;

    public CompositeDirectory(Directory directory1, Directory directory2) {
        super(directory1);
        this.directory1 = directory1;
        this.directory2 = directory2;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        directory1.deleteFile(name);
        directory2.deleteFile(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        IndexOutput output1 = directory1.createOutput(name, context);
        IndexOutput output2 = directory2.createOutput(name, context);
        return new CompositeIndexOutput(name, name, output1, output2, directory1);
    }

    @Override
    public void close() throws IOException {
        directory1.close();
        directory2.close();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return directory1.getPendingDeletions();
    }
}

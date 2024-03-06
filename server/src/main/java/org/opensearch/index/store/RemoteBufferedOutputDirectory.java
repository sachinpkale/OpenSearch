/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.*;
import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A {@code RemoteBufferedOutputDirectory} is an extension of RemoteDirectory which also provides an abstraction layer
 * for storing a list of files to a remote store.
 * Additionally, with this implementation, creation of new files is also allowed.
 * A remoteDirectory contains only files (no sub-folder hierarchy).
 *
 * @opensearch.internal
 */
public class RemoteBufferedOutputDirectory extends FSDirectory {
    public RemoteBufferedOutputDirectory(BlobContainer blobContainer) {
        super(Path.of("abc"), NoLockFactory.INSTANCE);

    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return new RemoteBufferedIndexOutput(name, null);
    }

    @Override
    public IndexInput openInput(String s, IOContext ioContext) throws IOException {
        return null;
    }
}

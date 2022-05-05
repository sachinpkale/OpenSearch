/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lucene.store.InputStreamIndexInput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Class for output to a file in a {@link RemoteDirectory}. Used for all output operations to the remote store.
 * Currently, only methods from {@link IndexOutput} that are required for uploading a segment file to remote store are
 * implemented. Remaining methods will be implemented as we open up remote store for other use cases like replication,
 * peer recovery etc.
 * ToDo: Extend ChecksumIndexInput
 * @see RemoteDirectory
 */
public class RemoteIndexOutput extends OutputStreamIndexOutput {

    private final BlobContainer blobContainer;
    private final ByteArrayOutputStream outputStream;

    public RemoteIndexOutput(String name, ByteArrayOutputStream outputStream, BlobContainer blobContainer) {
        super(name, name, outputStream, 8192);
        this.blobContainer = blobContainer;
        this.outputStream = outputStream;
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException {
        assert input instanceof IndexInput : "input should be instance of IndexInput";
        blobContainer.writeBlob(getName(), new InputStreamIndexInput((IndexInput) input, numBytes), numBytes, false);
    }

    /**
     * This is a no-op. Once segment file upload to the remote store is complete, we don't need to explicitly close
     * the stream. It is taken care by internal APIs of client of the remote store.
     */
    @Override
    public void close() throws IOException {
        byte[] contents = outputStream.toByteArray();
        blobContainer.writeBlob(getName(), new ByteArrayInputStream(contents), contents.length, false);
    }
}

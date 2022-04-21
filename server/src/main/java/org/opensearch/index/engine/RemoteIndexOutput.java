/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lucene.store.InputStreamIndexInput;

import java.io.IOException;

public class RemoteIndexOutput extends IndexOutput {

    private final BlobContainer blobContainer;

    public RemoteIndexOutput(String name, IOContext context, BlobContainer blobContainer) {
        super(name, name);
        this.blobContainer = blobContainer;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(byte[] byteArray, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        // throw new UnsupportedOperationException();
        // do nothing for now
    }

    @Override
    public long getFilePointer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getChecksum() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException {
        assert numBytes >= 0: "numBytes=" + numBytes;
        assert input instanceof IndexInput: "input should be instance of IndexInput";
        blobContainer.writeBlob(getName(), new InputStreamIndexInput((IndexInput) input, numBytes), numBytes, false);
    }

}

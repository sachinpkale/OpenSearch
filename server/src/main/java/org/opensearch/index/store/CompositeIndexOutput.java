/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class CompositeIndexOutput extends IndexOutput {
    private final IndexOutput output1;
    private final IndexOutput output2;
    private final Directory directory1;

    protected CompositeIndexOutput(String resourceDescription, String name, IndexOutput output1, IndexOutput output2, Directory directory1) {
        super(resourceDescription, name);
        this.output1 = output1;
        this.output2 = output2;
        this.directory1 = directory1;
    }

    @Override
    public void close() throws IOException {
        output1.close();
        IndexInput indexInput = directory1.openInput(this.getName(), IOContext.DEFAULT);
        output2.copyBytes(indexInput, indexInput.length());
    }

    @Override
    public long getFilePointer() {
        return output1.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
        return output1.getChecksum();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        output1.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        output1.writeBytes(b, offset, length);
    }
}

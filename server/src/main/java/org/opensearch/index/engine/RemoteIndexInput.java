/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;

public class RemoteIndexInput extends IndexInput {

    private final InputStream inputStream;

    public RemoteIndexInput(String name, InputStream inputStream) throws IOException {
        super(name);
        this.inputStream = inputStream;
    }

    @Override
    public byte readByte() throws IOException {
        return inputStream.readNBytes(1)[0];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        inputStream.readNBytes(b, offset, len);
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public long getFilePointer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long pos) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long length() {
        try {
            return inputStream.available();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException();
    }
}

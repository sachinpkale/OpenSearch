/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class for input from a file in a {@link RemoteDirectory}. Used for all input operations from the remote store.
 * Currently, only methods from {@link IndexInput} that are required for reading a file from remote store are
 * implemented. Remaining methods will be implemented as we open up remote store for other use cases like replication,
 * peer recovery etc.
 * ToDo: Extend ChecksumIndexInput
 * @see RemoteDirectory
 */
public class RemoteIndexInput extends IndexInput {

    private final InputStream inputStream;
    private final long size;

    public RemoteIndexInput(String name, InputStream inputStream, long size) {
        super(name);
        this.inputStream = inputStream;
        this.size = size;
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
    public long length() {
        return size;
    }

    @Override
    public void seek(long pos) throws IOException {
        inputStream.skipNBytes(pos);
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexInput unmodified.
     * This method is not implemented as it is not used for the file transfer to/from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public long getFilePointer() {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexInput unmodified.
     * This method is not implemented as it is not used for the file transfer to/from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException();
    }
}

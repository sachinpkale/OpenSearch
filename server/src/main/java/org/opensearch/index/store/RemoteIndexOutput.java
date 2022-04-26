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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lucene.store.InputStreamIndexInput;

import java.io.IOException;

/**
 * Class for output to a file in a {@link RemoteDirectory}. Used for all output operations to the remote store.
 * Currently, only methods from {@link IndexOutput} that are required for uploading a segment file to remote store are
 * implemented. Remaining methods will be implemented as we open up remote store for other use cases like replication,
 * peer recovery etc.
 * ToDo: Extend ChecksumIndexInput
 * @see RemoteDirectory
 */
public class RemoteIndexOutput extends IndexOutput {

    private final BlobContainer blobContainer;

    public RemoteIndexOutput(String name, BlobContainer blobContainer) {
        super(name, name);
        this.blobContainer = blobContainer;
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException {
        assert numBytes >= 0: "numBytes=" + numBytes;
        assert input instanceof IndexInput: "input should be instance of IndexInput";
        blobContainer.writeBlob(getName(), new InputStreamIndexInput((IndexInput) input, numBytes), numBytes, false);
    }

    /**
     * This is a no-op. Once segment file upload to the remote store is complete, we don't need to explicitly close
     * the stream. It is taken care by internal APIs of client of the remote store.
     */
    @Override
    public void close() throws IOException {
        // do nothing
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexOutput unmodified.
     * This method is not implemented as it is not used for the file transfer to/from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeByte(byte b) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexOutput unmodified.
     * This method is not implemented as it is not used for the file transfer to/from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeBytes(byte[] byteArray, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexOutput unmodified.
     * This method is not implemented as it is not used for the file transfer to/from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public long getFilePointer() {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexOutput unmodified.
     * This method is not implemented as it is not directly used for the file transfer to/from the remote store.
     * But the checksum is important to verify integrity of the data and that means implementing this method will
     * be required for the segment upload as well.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public long getChecksum() throws IOException {
        throw new UnsupportedOperationException();
    }

}

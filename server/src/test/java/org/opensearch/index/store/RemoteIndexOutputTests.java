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
import org.junit.Before;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class RemoteIndexOutputTests extends OpenSearchTestCase {
    private static final String FILENAME = "segment_1";

    private BlobContainer blobContainer;

    private RemoteIndexOutput remoteIndexOutput;

    @Before
    public void setup() {
        blobContainer = mock(BlobContainer.class);
        remoteIndexOutput = new RemoteIndexOutput(FILENAME, blobContainer);
    }

    public void testCopyBytes() throws IOException {
        IndexInput indexInput = mock(IndexInput.class);
        remoteIndexOutput.copyBytes(indexInput, 100);

        verify(blobContainer).writeBlob(eq(FILENAME), any(InputStreamIndexInput.class), eq(100L), eq(false));
    }

    public void testCopyBytesIOException() throws IOException {
        doThrow(new IOException("Error writing")).when(blobContainer)
            .writeBlob(eq(FILENAME), any(InputStreamIndexInput.class), eq(100L), eq(false));

        IndexInput indexInput = mock(IndexInput.class);
        assertThrows(IOException.class, () -> remoteIndexOutput.copyBytes(indexInput, 100));
    }

    public void testWriteByte() {
        byte b = 10;
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexOutput.writeByte(b));
    }

    public void testWriteBytes() {
        byte[] buffer = new byte[10];
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexOutput.writeBytes(buffer, 50, 60));
    }

    public void testGetFilePointer() {
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexOutput.getFilePointer());
    }

    public void testGetChecksum() {
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexOutput.getChecksum());
    }
}

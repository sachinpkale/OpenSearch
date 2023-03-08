/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.junit.Before;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit Tests for {@link RemoteSegmentMetadataHandler}
 */
public class RemoteSegmentMetadataHandlerTests extends OpenSearchTestCase {
    private RemoteSegmentMetadataHandler remoteSegmentMetadataHandler;

    @Before
    public void setup() throws IOException {
        remoteSegmentMetadataHandler = new RemoteSegmentMetadataHandler();
    }

    public void testReadContent() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> expectedOutput = getDummyData();
        RemoteSegmentMetadata.writeContent(indexOutput, new RemoteSegmentMetadata(expectedOutput, System.currentTimeMillis()));
        indexOutput.close();
        RemoteSegmentMetadata metadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );
        assertEquals(expectedOutput, metadata.getMetadata());
    }

    public void testWriteContent() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> expectedOutput = getDummyData();
        remoteSegmentMetadataHandler.writeContent(indexOutput, new RemoteSegmentMetadata(expectedOutput, System.currentTimeMillis()));
        indexOutput.close();
        Map<String, String> actualOutput = new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
            .readMapOfStrings();
        assertEquals(expectedOutput.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString())), actualOutput);
    }

    private Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> getDummyData() {
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> expectedOutput = new HashMap<>();
        String prefix = "_0";
        expectedOutput.put(
            prefix + ".cfe",
            new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(prefix + ".cfe", prefix + ".cfe__" + UUIDs.base64UUID(), String.valueOf(randomIntBetween(1000, 5000)))
        );
        expectedOutput.put(
            prefix + ".cfs",
            new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(prefix + ".cfs", prefix + ".cfs__" + UUIDs.base64UUID(), String.valueOf(randomIntBetween(1000, 5000)))
        );
        return expectedOutput;
    }
}

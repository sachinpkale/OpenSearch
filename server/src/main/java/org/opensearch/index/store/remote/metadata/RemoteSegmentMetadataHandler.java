/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.io.IndexIOStreamHandler;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

/**
 * Handler for {@link RemoteSegmentMetadata}
 *
 * @opensearch.internal
 */
public class RemoteSegmentMetadataHandler implements IndexIOStreamHandler<RemoteSegmentMetadata> {
    /**
     * Reads metadata content from metadata file input stream and parsed into {@link RemoteSegmentMetadata}
     * @param indexInput metadata file input stream with {@link IndexInput#getFilePointer()} pointing to metadata content
     * @return {@link RemoteSegmentMetadata}
     */
    @Override
    public RemoteSegmentMetadata readContent(IndexInput indexInput) throws IOException {
        return new RemoteSegmentMetadata(
            indexInput.readMapOfStrings().entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(entry.getValue())
                    )
                ),
            indexInput.readLong()
        );
    }

    /**
     * Writes metadata to file output stream
     * @param indexOutput metadata file input stream
     * @param content {@link RemoteSegmentMetadata} from which metadata content would be generated
     */
    @Override
    public void writeContent(IndexOutput indexOutput, RemoteSegmentMetadata content) throws IOException {
        indexOutput.writeMapOfStrings(content.getMetadata().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString())));
        indexOutput.writeLong(content.getMetadataCreationTimestamp());
    }
}

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
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

/**
 * Metadata object for Remote Segment
 *
 * @opensearch.internal
 */
public class RemoteSegmentMetadata {
    /**
     * Latest supported version of metadata
     */
    public static final int CURRENT_VERSION = 1;
    /**
     * Metadata codec
     */
    public static final String METADATA_CODEC = "segment_md";

    /**
     * Data structure holding metadata content
     */
    private final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata;

    /**
     * Timestamp of the file at the time of creation
     */
    private final Long timestamp;

    public RemoteSegmentMetadata(Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata, Long timestamp) {
        this.metadata = metadata;
        this.timestamp = timestamp;
    }

    /**
     * Exposes underlying metadata content data structure.
     * @return {@code metadata}
     */
    public Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> getMetadata() {
        return this.metadata;
    }

    /**
     * Get creation timestamp of the metadata
     * @return {@code timestamp}
     */
    public long getMetadataCreationTimestamp() {
        return timestamp;
    }
}

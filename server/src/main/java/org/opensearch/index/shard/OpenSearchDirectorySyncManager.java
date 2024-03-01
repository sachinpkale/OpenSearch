/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public final class OpenSearchDirectorySyncManager {

    private static final Logger logger = LogManager.getLogger(OpenSearchDirectorySyncManager.class);

    public static boolean syncCacheFilesToStorage(SegmentInfos segmentInfos, Directory cache, RemoteSegmentStoreDirectory storage) throws InterruptedException, IOException {
        Collection<String> localSegmentsPostRefresh = segmentInfos.files(true);
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean successful = new AtomicBoolean(false);
        if (isRefreshAfterCommit(cache, storage)) {
            storage.deleteStaleSegmentsAsync(5);
        }
        ActionListener<Void> segmentUploadsCompletedListener = new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                try {
                    logger.debug("New segments upload successful");
                    // Start metadata file upload
                    uploadMetadata(localSegmentsPostRefresh, segmentInfos, cache, storage);
                    logger.debug("Metadata upload successful");
                    // At this point since we have uploaded new segments, segment infos and segment metadata file,
                    // along with marking minSeqNoToKeep, upload has succeeded completely.
                    successful.set(true);
                } catch (Exception e) {
                    // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                    // as part of exponential back-off retry logic. This should not affect durability of the indexed data
                    // with remote trans-log integration.
                    logger.warn("Exception in post new segment upload actions", e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Exception while uploading new segments to the remote segment store", e);
            }
        }, latch);

        // Start the segments files upload
        uploadNewSegments(localSegmentsPostRefresh, segmentUploadsCompletedListener, cache, storage);
        latch.await();
        return successful.get();
    }

    public static void syncStorageFilesToCache(Directory cache, RemoteSegmentStoreDirectory storage) {

    }

    private static boolean isRefreshAfterCommit(Directory cache, RemoteSegmentStoreDirectory storage) throws IOException {
        String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(cache);
        return (lastCommittedLocalSegmentFileName != null
            && !storage.containsFile(lastCommittedLocalSegmentFileName, getChecksumOfLocalFile(lastCommittedLocalSegmentFileName, cache)));
    }


    private static void uploadNewSegments(
        Collection<String> localSegmentsPostRefresh,
        ActionListener<Void> listener,
        Directory cache,
        RemoteSegmentStoreDirectory storage
    ) {
        Collection<String> filteredFiles = localSegmentsPostRefresh.stream().filter(file -> !skipUpload(file, cache, storage)).collect(Collectors.toList());
        if (filteredFiles.isEmpty()) {
            logger.debug("No new segments to upload in uploadNewSegments");
            listener.onResponse(null);
            return;
        }

        logger.debug("Effective new segments files to upload {}", filteredFiles);
        ActionListener<Collection<Void>> mappedListener = ActionListener.map(listener, resp -> null);
        GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(mappedListener, filteredFiles.size());

        for (String src : filteredFiles) {
            // Initializing listener here to ensure that the stats increment operations are thread-safe
            ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                batchUploadListener.onResponse(resp);
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                batchUploadListener.onFailure(ex);
            });
            storage.copyFrom(cache, src, IOContext.DEFAULT, aggregatedListener);
        }
    }

    private static boolean skipUpload(String file, Directory cache, Directory storage) {
        try {
            // Exclude files that are already uploaded and the exclude files to come up with the list of files to be uploaded.
            return ((RemoteSegmentStoreDirectory)storage).containsFile(file, getChecksumOfLocalFile(file, cache));
        } catch (IOException e) {
            logger.error(
                "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                file
            );
        }
        return false;
    }

    private static String getChecksumOfLocalFile(String file, Directory cache) throws IOException {
        try (IndexInput indexInput = cache.openInput(file, IOContext.DEFAULT)) {
                return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }


    private static void uploadMetadata(Collection<String> localSegmentsPostRefresh, SegmentInfos segmentInfos, Directory cacheDirectory, RemoteSegmentStoreDirectory storageDirectory)
        throws IOException {
        storageDirectory.uploadMetadata(
            localSegmentsPostRefresh,
            segmentInfos,
            cacheDirectory,
            1,
            ReplicationCheckpoint.empty(new ShardId("index-1", "index-uuid", 1)),
            "dummyNodeId"
        );
    }
}


/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.*;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.*;
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

    public static boolean syncCacheFilesToStorage(Collection<String> localSegmentsPostRefresh, Directory cache, RemoteSegmentStoreDirectory storage) throws InterruptedException, IOException {
        StandardDirectoryReader directoryReader
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

    public static void syncStorageFilesToCache(OpenSearchDirectory openSearchDirectory) throws IOException {
        Directory cache = openSearchDirectory.getCache();
        RemoteSegmentStoreDirectory storage = (RemoteSegmentStoreDirectory) openSearchDirectory.getStorage();
        // We need to call RemoteSegmentStoreDirectory.init() in order to get latest metadata of the files that
        // are uploaded to the remote segment store.
        RemoteSegmentMetadata metadata = storage.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = storage
            .getSegmentsUploadedToRemoteStore()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(IndexFileNames.SEGMENTS) == false)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (uploadedSegments.isEmpty() == false) {
            try {
                copySegmentFiles(cache, storage, uploadedSegments);
                try (final ChecksumIndexInput input = toIndexInput(metadata.getSegmentInfosBytes())) {
                    SegmentInfos segmentInfos = SegmentInfos.readCommit(cache, input, metadata.getGeneration());
                    segmentInfos.commit(cache);
                }
            } catch (IOException e) {
                throw new RuntimeException("Exception while copying segment files from remote segment store", e);
            }
        }
    }

    private static ChecksumIndexInput toIndexInput(byte[] input) {
        return new BufferedChecksumIndexInput(new ByteArrayIndexInput("Snapshot of SegmentInfos", input));
    }

    private static void copySegmentFiles(Directory cache, RemoteSegmentStoreDirectory storage, Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments) throws IOException {
        Set<String> toDownloadSegments = new HashSet<>();
        Set<String> skippedSegments = new HashSet<>();
        try {
            for (String file : cache.listAll()) {
                cache.deleteFile(file);
            }
            for (String file : uploadedSegments.keySet()) {
                long checksum = Long.parseLong(uploadedSegments.get(file).getChecksum());
                if (localDirectoryContains(cache, file, checksum) == false) {
                    toDownloadSegments.add(file);
                } else {
                    skippedSegments.add(file);
                }
            }
            for (String file: toDownloadSegments) {
                try {
                    logger.info("Dowloading file: " + file);
                    cache.copyFrom(storage, file, file, IOContext.DEFAULT);
                } catch (Exception e) {
                    throw new IOException("Error occurred when downloading segments from remote store", e);
                }
            }
        } finally {
            logger.info("Downloaded segments here: {}", toDownloadSegments);
            logger.info("Skipped download for segments here: {}", skippedSegments);
        }
    }

    private static boolean localDirectoryContains(Directory localDirectory, String file, long checksum) throws IOException {
        try (IndexInput indexInput = localDirectory.openInput(file, IOContext.DEFAULT)) {
            if (checksum == CodecUtil.retrieveChecksum(indexInput)) {
                return true;
            } else {
                logger.warn("Checksum mismatch between local and remote segment file: {}, will override local file", file);
                localDirectory.deleteFile(file);
            }
        } catch (NoSuchFileException | FileNotFoundException e) {
            logger.debug("File {} does not exist in local FS, downloading from remote store", file);
        } catch (IOException e) {
            logger.warn("Exception while reading checksum of file: {}, this can happen if file is corrupted", file);
            // For any other exception on reading checksum, we delete the file to re-download again
            localDirectory.deleteFile(file);
        }
        return false;
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
            new ReplicationCheckpoint(new ShardId("index-1", "index-uuid", 1), 0, segmentInfos.getGeneration(), segmentInfos.version, 0L, "", Collections.emptyMap()),
            "dummyNodeId"
        );
    }
}


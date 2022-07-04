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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.EngineException;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    public static final String COMMITTED_SEGMENTINFOS_FILENAME = "segments_";
    public static final String REFRESHED_SEGMENTINFOS_FILENAME = "refreshed_segments_";
    public static final int DELETE_PERIOD_IN_MINS = 15;
    private static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final Directory remoteDirectory;
    private final ConcurrentHashMap<String, String> segmentsUploadedToRemoteStore;
    private final Scheduler.Cancellable scheduler;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(IndexShard indexShard) throws IOException {
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate()).getDelegate();
        // ToDo: Handle failures in reading list of files (GitHub #3397)
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(Arrays.stream(remoteDirectory.listAll()).collect(Collectors.toMap(Function.identity(), Function.identity())));
        this.scheduler = scheduleStaleSegmentDeletion(indexShard.getThreadPool());
    }

    private Scheduler.Cancellable scheduleStaleSegmentDeletion(ThreadPool threadPool) {
        return threadPool.scheduleWithFixedDelay(() -> {
            try {
                deleteStaleSegments();
            } catch(AlreadyClosedException e) {
                logger.info("Directory is closed. Stopping the scheduler");
                this.scheduler.cancel();
            } catch (Throwable t) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Exception while deleting stale segments from the remote segment store, will retry again after {} minutes",
                        DELETE_PERIOD_IN_MINS
                    ),
                    t
                );
            }
        }, TimeValue.timeValueMinutes(DELETE_PERIOD_IN_MINS), ThreadPool.Names.REMOTE_STORE);
    }

    /**
     * Deletes segment files from remote store which are not part of local filesystem.
     * @throws IOException in case of I/O error in reading list of remote files
     */
    // Visible for testing
    synchronized void deleteStaleSegments() throws IOException {
        Set<String> localFiles = Set.of(storeDirectory.listAll());
        Set<String> remoteSegments = Set.of(remoteDirectory.listAll());

        Collection<String> committedSegmentFiles = getRemoteSegmentsFromCheckpoint(remoteSegments, COMMITTED_SEGMENTINFOS_FILENAME);
        Collection<String> refreshedSegmentFiles = getRemoteSegmentsFromCheckpoint(remoteSegments, REFRESHED_SEGMENTINFOS_FILENAME);

        remoteSegments.stream()
            .filter(file -> !committedSegmentFiles.contains(file))
            .filter(file -> !refreshedSegmentFiles.contains(file))
            .filter(file -> !localFiles.contains(file))
            .forEach(file -> {
                try {
                    remoteDirectory.deleteFile(file);
                    segmentsUploadedToRemoteStore.remove(file);
                } catch (IOException e) {
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(() -> new ParameterizedMessage("Exception while deleting file {} from the remote segment store", file), e);
                }
            });
    }

    public Collection<String> getRemoteSegmentsFromCheckpoint(Set<String> remoteSegments, String prefix) {
        Set<String> allCheckpoints = remoteSegments.stream()
            .filter(file -> file.startsWith(prefix))
            .collect(Collectors.toSet());
        Optional<Integer> lastGeneration = allCheckpoints.stream()
            .map(file -> Integer.parseInt(file.substring(prefix.length()), Character.MAX_RADIX))
            .max(Comparator.naturalOrder());
        if (lastGeneration.isEmpty()) {
            logger.info("{}_N is yet to be uploaded to the remote store", prefix);
            return Set.of();
        }
        String lastCheckpoint = prefix + Integer.toString(lastGeneration.get(), Character.MAX_RADIX);
        try (ChecksumIndexInput input = remoteDirectory.openChecksumInput(lastCheckpoint, IOContext.READ)) {
            try {
                SegmentInfos segmentInfos = SegmentInfos.readCommit(remoteDirectory, input, lastGeneration.get());
                return segmentInfos.files(true);
            } catch (EOFException | NoSuchFileException | FileNotFoundException e) {
                throw new CorruptIndexException("Unexpected file read error while reading index.", input, e);
            }
        } catch(IOException e) {
            logger.warn(
                () -> new ParameterizedMessage("Error reading file {} to get the list of files", lastCheckpoint),
                e
            );
        }
        return Set.of();
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Do Nothing
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     * @param didRefresh true if the refresh opened a new reference
     * @throws IOException in case of I/O error in reading list of local files
     */
    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        try {
            String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
            if(!segmentsUploadedToRemoteStore.containsKey(lastCommittedLocalSegmentFileName)) {
                Collection<String> committedLocalFiles = SegmentInfos.readCommit(storeDirectory, lastCommittedLocalSegmentFileName).files(true);
                boolean uploadStatus = uploadNewSegments(committedLocalFiles);
                if(uploadStatus) {
                    remoteDirectory.copyFrom(storeDirectory, lastCommittedLocalSegmentFileName, lastCommittedLocalSegmentFileName, IOContext.DEFAULT);
                    segmentsUploadedToRemoteStore.put(lastCommittedLocalSegmentFileName, lastCommittedLocalSegmentFileName);
                }
            } else {
                logger.info("Latest commit point {} is present in remote store", lastCommittedLocalSegmentFileName);
            }
            try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();
                Collection<String> refreshedLocalFiles = segmentInfos.files(true);
                boolean uploadStatus = uploadNewSegments(refreshedLocalFiles);
                if (uploadStatus) {
                    uploadRemoteSegmentsMetadata(segmentInfos);
                }
            } catch (EngineException e) {
                logger.warn("Exception while reading SegmentInfosSnapshot", e);
            }
        } catch (IOException e) {
            // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
            // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
            logger.warn("Exception while uploading new segments to the remote segment store", e);
        }
    }

    // Visible for testing
    synchronized boolean uploadNewSegments(Collection<String> localFiles) throws IOException {
        AtomicBoolean uploadSuccess = new AtomicBoolean(true);
        localFiles.stream()
            .filter(file -> !EXCLUDE_FILES.contains(file))
            .filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .filter(file -> !file.startsWith(COMMITTED_SEGMENTINFOS_FILENAME))
            .filter(file -> !segmentsUploadedToRemoteStore.containsKey(file))
            .forEach(file -> {
                try {
                    remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
                    segmentsUploadedToRemoteStore.put(file, file);
                } catch (NoSuchFileException e) {
                    logger.info("The file {} does not exist anymore. It can happen in case of temp files", file);
                } catch (IOException e) {
                    uploadSuccess.set(false);
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(
                        () -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file),
                        e
                    );
                }
            });

        return uploadSuccess.get();
    }

    // Visible for testing
    synchronized void uploadRemoteSegmentsMetadata(SegmentInfos segmentInfos) throws IOException {
        String segmentInfosFileName = REFRESHED_SEGMENTINFOS_FILENAME + Long.toString(segmentInfos.getGeneration(), Character.MAX_RADIX);
        try {
            storeDirectory.deleteFile(segmentInfosFileName);
        } catch (NoSuchFileException e) {
            logger.info(
                "File {} is missing in local filesystem. This can happen for the first refresh of the generation",
                segmentInfosFileName
            );
        }
        IndexOutput indexOutput = storeDirectory.createOutput(segmentInfosFileName, IOContext.DEFAULT);
        segmentInfos.write(indexOutput);
        indexOutput.close();
        storeDirectory.sync(Collections.singleton(segmentInfosFileName));
        remoteDirectory.copyFrom(storeDirectory, segmentInfosFileName, segmentInfosFileName, IOContext.DEFAULT);
        segmentsUploadedToRemoteStore.put(segmentInfosFileName, segmentInfosFileName);
        Set<String> staleSegmentInfosFiles = segmentsUploadedToRemoteStore.keySet().stream()
            .filter(file -> file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .filter(file -> !file.equals(segmentInfosFileName))
            .collect(Collectors.toSet());
        staleSegmentInfosFiles.forEach(file -> {
            try {
                storeDirectory.deleteFile(file);
            } catch(NoSuchFileException e) {
                segmentsUploadedToRemoteStore.remove(file);
                logger.warn(() -> new ParameterizedMessage("Delete failed as file {} does not exist in local store", file), e);
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("Exception while deleting file {} from the local store", file), e);
            }
        });
    }

    // Visible for testing
    Map<String, String> getUploadedSegments() {
        return this.segmentsUploadedToRemoteStore;
    }
}

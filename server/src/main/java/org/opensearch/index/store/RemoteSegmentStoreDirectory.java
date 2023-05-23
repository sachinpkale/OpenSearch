/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.store.lockmanager.RemoteStoreCommitLevelLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.FileLockInfo;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandler;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.HashSet;
import java.util.Optional;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A RemoteDirectory extension for remote segment store. We need to make sure we don't overwrite a segment file once uploaded.
 * In order to prevent segment overwrite which can occur due to two primary nodes for the same shard at the same time,
 * a unique suffix is added to the uploaded segment file. This class keeps track of filename of segments stored
 * in remote segment store vs filename in local filesystem and provides the consistent Directory interface so that
 * caller will be accessing segment files in the same way as {@code FSDirectory}. Apart from storing actual segment files,
 * remote segment store also keeps track of refresh checkpoints as metadata in a separate path which is handled by
 * another instance of {@code RemoteDirectory}.
 * @opensearch.internal
 */
public final class RemoteSegmentStoreDirectory extends FilterDirectory implements RemoteStoreCommitLevelLockManager {
    /**
     * Each segment file is uploaded with unique suffix.
     * For example, _0.cfe in local filesystem will be uploaded to remote segment store as _0.cfe__gX7bNIIBrs0AUNsR2yEG
     */
    public static final String SEGMENT_NAME_UUID_SEPARATOR = "__";

    public static final MetadataFilenameUtils.MetadataFilenameComparator METADATA_FILENAME_COMPARATOR =
        new MetadataFilenameUtils.MetadataFilenameComparator();

    /**
     * remoteDataDirectory is used to store segment files at path: cluster_UUID/index_UUID/shardId/segments/data
     */
    private final RemoteDirectory remoteDataDirectory;
    /**
     * remoteMetadataDirectory is used to store metadata files at path: cluster_UUID/index_UUID/shardId/segments/metadata
     */
    private final RemoteDirectory remoteMetadataDirectory;

    private final RemoteStoreLockManager mdLockManager;

    /**
     * To prevent explosion of refresh metadata files, we replace refresh files for the given primary term and generation
     * This is achieved by uploading refresh metadata file with the same UUID suffix.
     */
    private String commonFilenameSuffix;

    /**
     * Keeps track of local segment filename to uploaded filename along with other attributes like checksum.
     * This map acts as a cache layer for uploaded segment filenames which helps avoid calling listAll() each time.
     * It is important to initialize this map on creation of RemoteSegmentStoreDirectory and update it on each upload and delete.
     */
    private Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    private static final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new RemoteSegmentMetadataHandler(),
        RemoteSegmentMetadata.CURRENT_VERSION,
        RemoteSegmentMetadata.METADATA_CODEC
    );

    private static final Logger logger = LogManager.getLogger(RemoteSegmentStoreDirectory.class);

    public RemoteSegmentStoreDirectory(
        RemoteDirectory remoteDataDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager
    ) throws IOException {
        super(remoteDataDirectory);
        this.remoteDataDirectory = remoteDataDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.mdLockManager = mdLockManager;
        init();
    }

    /**
     * Initializes the cache which keeps track of all the segment files uploaded to the remote segment store.
     * As this cache is specific to an instance of RemoteSegmentStoreDirectory, it is possible that cache becomes stale
     * if another instance of RemoteSegmentStoreDirectory is used to upload/delete segment files.
     * It is caller's responsibility to call init() again to ensure that cache is properly updated.
     * @throws IOException if there were any failures in reading the metadata file
     */
    public RemoteSegmentMetadata init() throws IOException {
        this.commonFilenameSuffix = UUIDs.base64UUID();
        RemoteSegmentMetadata remoteSegmentMetadata = readLatestMetadataFile();
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
        return remoteSegmentMetadata;
    }

    /**
     * Read the latest metadata file to get the list of segments uploaded to the remote segment store.
     * We upload a metadata file per refresh, but it is not unique per refresh. Refresh metadata file is unique for a given commit.
     * The format of refresh metadata filename is: refresh_metadata__PrimaryTerm__Generation__UUID
     * Refresh metadata files keep track of active segments for the shard at the time of refresh.
     * In order to get the list of segment files uploaded to the remote segment store, we need to read the latest metadata file.
     * Each metadata file contains a map where
     *      Key is - Segment local filename and
     *      Value is - local filename::uploaded filename::checksum
     * @return Map of segment filename to uploaded filename with checksum
     * @throws IOException if there were any failures in reading the metadata file
     */
    private RemoteSegmentMetadata readLatestMetadataFile() throws IOException {
        Map<String, UploadedSegmentMetadata> segmentMetadataMap = new HashMap<>();
        RemoteSegmentMetadata remoteSegmentMetadata = null;

        Collection<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefix(MetadataFilenameUtils.METADATA_PREFIX);
        Optional<String> latestMetadataFile = metadataFiles.stream().max(METADATA_FILENAME_COMPARATOR);

        if (latestMetadataFile.isPresent()) {
            logger.info("Reading latest Metadata file {}", latestMetadataFile.get());
            remoteSegmentMetadata = readMetadataFile(latestMetadataFile.get());
        } else {
            logger.info("No metadata file found, this can happen for new index with no data uploaded to remote segment store");
        }

        return remoteSegmentMetadata;
    }

    private RemoteSegmentMetadata readMetadataFile(String metadataFilename) throws IOException {
        try (IndexInput indexInput = remoteMetadataDirectory.openInput(metadataFilename, IOContext.DEFAULT)) {
            byte[] metadataBytes = new byte[(int) indexInput.length()];
            indexInput.readBytes(metadataBytes, 0, (int) indexInput.length());
            RemoteSegmentMetadata metadata = metadataStreamWrapper.readStream(new ByteArrayIndexInput(metadataFilename, metadataBytes));
            return metadata;
        }
    }

    /**
     * Metadata of a segment that is uploaded to remote segment store.
     */
    public static class UploadedSegmentMetadata {
        // Visible for testing
        static final String SEPARATOR = "::";

        private final String originalFilename;
        private final String uploadedFilename;
        private final String checksum;
        private final long length;

        UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum, long length) {
            this.originalFilename = originalFilename;
            this.uploadedFilename = uploadedFilename;
            this.checksum = checksum;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.join(SEPARATOR, originalFilename, uploadedFilename, checksum, String.valueOf(length));
        }

        public String getChecksum() {
            return this.checksum;
        }

        public long getLength() {
            return this.length;
        }

        public static UploadedSegmentMetadata fromString(String uploadedFilename) {
            String[] values = uploadedFilename.split(SEPARATOR);
            return new UploadedSegmentMetadata(values[0], values[1], values[2], Long.parseLong(values[3]));
        }
    }

    /**
     * Contains utility methods that provide various parts of metadata filename along with comparator
     * Each metadata filename is of format: PREFIX__PrimaryTerm__Generation__UUID
     */
    static class MetadataFilenameUtils {
        public static final String SEPARATOR = "__";
        public static final String METADATA_PREFIX = "metadata";

        /**
         * Comparator to sort the metadata filenames. The order of sorting is: Primary Term, Generation, UUID
         * Even though UUID sort does not provide any info on recency, it provides a consistent way to sort the filenames.
         */
        static class MetadataFilenameComparator implements Comparator<String> {
            @Override
            public int compare(String first, String second) {
                String[] firstTokens = first.split(SEPARATOR);
                String[] secondTokens = second.split(SEPARATOR);
                if (!firstTokens[0].equals(secondTokens[0])) {
                    return firstTokens[0].compareTo(secondTokens[0]);
                }
                long firstPrimaryTerm = getPrimaryTerm(firstTokens);
                long secondPrimaryTerm = getPrimaryTerm(secondTokens);
                if (firstPrimaryTerm != secondPrimaryTerm) {
                    return firstPrimaryTerm > secondPrimaryTerm ? 1 : -1;
                } else {
                    long firstGeneration = getGeneration(firstTokens);
                    long secondGeneration = getGeneration(secondTokens);
                    if (firstGeneration != secondGeneration) {
                        return firstGeneration > secondGeneration ? 1 : -1;
                    } else {
                        return getUuid(firstTokens).compareTo(getUuid(secondTokens));
                    }
                }
            }
        }

        static String getMetadataFilePrefixForCommit(long primaryTerm, long generation) {
            return String.join(SEPARATOR, METADATA_PREFIX, Long.toString(primaryTerm), Long.toString(generation, Character.MAX_RADIX));
        }

        // Visible for testing
        static String getMetadataFilename(long primaryTerm, long generation, String uuid) {
            return String.join(SEPARATOR, getMetadataFilePrefixForCommit(primaryTerm, generation), uuid);
        }

        // Visible for testing
        static long getPrimaryTerm(String[] filenameTokens) {
            return Long.parseLong(filenameTokens[1]);
        }

        // Visible for testing
        static long getGeneration(String[] filenameTokens) {
            return Long.parseLong(filenameTokens[2], Character.MAX_RADIX);
        }

        // Visible for testing
        static String getUuid(String[] filenameTokens) {
            return filenameTokens[3];
        }
    }

    /**
     * Returns list of all the segment files uploaded to remote segment store till the last refresh checkpoint.
     * Any segment file that is uploaded without corresponding metadata file will not be visible as part of listAll().
     * We chose not to return cache entries for listAll as cache can have entries for stale segments as well.
     * Even if we plan to delete stale segments from remote segment store, it will be a periodic operation.
     * @return segment filenames stored in remote segment store
     * @throws IOException if there were any failures in reading the metadata file
     */
    @Override
    public String[] listAll() throws IOException {
        return readLatestMetadataFile().getMetadata().keySet().toArray(new String[0]);
    }

    /**
     * Delete segment file from remote segment store.
     * @param name the name of an existing segment file in local filesystem.
     * @throws IOException if the file exists but could not be deleted.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            remoteDataDirectory.deleteFile(remoteFilename);
            segmentsUploadedToRemoteStore.remove(name);
        }
    }

    /**
     * Returns the byte length of a segment file in the remote segment store.
     * @param name the name of an existing segment file in local filesystem.
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist in the cache or remote segment store
     */
    @Override
    public long fileLength(String name) throws IOException {
        if (segmentsUploadedToRemoteStore.containsKey(name)) {
            return segmentsUploadedToRemoteStore.get(name).getLength();
        }
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            return remoteDataDirectory.fileLength(remoteFilename);
        } else {
            throw new NoSuchFileException(name);
        }
    }

    /**
     * Creates and returns a new instance of {@link RemoteIndexOutput} which will be used to copy files to the remote
     * segment store.
     * @param name the name of the file to create.
     * @throws IOException in case of I/O error
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return remoteDataDirectory.createOutput(getNewRemoteSegmentFilename(name), context);
    }

    /**
     * Opens a stream for reading an existing file and returns {@link RemoteIndexInput} enclosing the stream.
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist either in cache or remote segment store
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            return remoteDataDirectory.openInput(remoteFilename, context);
        } else {
            throw new NoSuchFileException(name);
        }
    }

    /**
     * This acquires a lock on a given commit by creating a lock file in lock directory using {@code FileLockInfo}
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation Commit Generation
     * @param acquirerId Lock Acquirer ID which wants to acquire lock on the commit.
     * @throws IOException will be thrown in case i) listing file failed or ii) Writing the lock file failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public void acquireLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);

        mdLockManager.acquire(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    /**
     * Releases a lock which was acquired on given segment commit.
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation Commit Generation
     * @param acquirerId Acquirer ID for which lock needs to be released.
     * @throws IOException will be thrown in case i) listing lock files failed or ii) deleting the lock file failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public void releaseLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        mdLockManager.release(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    /**
     * Checks if a specific commit have any corresponding lock file.
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation Commit Generation
     * @return True if there is at least one lock for given primary term and generation.
     * @throws IOException will be thrown in case listing lock files failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public Boolean isLockAcquired(long primaryTerm, long generation) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        return mdLockManager.isAcquired(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).build());
    }

    // Visible for testing
    String getMetadataFileForCommit(long primaryTerm, long generation) throws IOException {
        Collection<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefix(
            MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation)
        );

        if (metadataFiles.isEmpty()) {
            throw new NoSuchFileException(
                "Metadata file is not present for given primary term " + primaryTerm + " and generation " + generation
            );
        }
        if (metadataFiles.size() != 1) {
            throw new IllegalStateException(
                "there should be only one metadata file for given primary term "
                    + primaryTerm
                    + "and generation "
                    + generation
                    + " but found "
                    + metadataFiles.size()
            );
        }
        return metadataFiles.iterator().next();
    }

    public void copyFrom(Directory from, String src, String dest, IOContext context, boolean useCommonSuffix, String checksum)
        throws IOException {
        String remoteFilename;
        if (useCommonSuffix) {
            remoteFilename = dest + SEGMENT_NAME_UUID_SEPARATOR + this.commonFilenameSuffix;
        } else {
            remoteFilename = getNewRemoteSegmentFilename(dest);
        }
        remoteDataDirectory.copyFrom(from, src, remoteFilename, context);
        UploadedSegmentMetadata segmentMetadata = new UploadedSegmentMetadata(src, remoteFilename, checksum, from.fileLength(src));
        segmentsUploadedToRemoteStore.put(src, segmentMetadata);
    }

    public void copyFrom(Directory from, String src, String dest, IOContext context, boolean useCommonSuffix) throws IOException {
        copyFrom(from, src, dest, context, useCommonSuffix, getChecksumOfLocalFile(from, src));
    }

    /**
     * Copies an existing src file from directory from to a non-existent file dest in this directory.
     * Once the segment is uploaded to remote segment store, update the cache accordingly.
     */
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        copyFrom(from, src, dest, context, false);
    }

    /**
     * Checks if the file exists in the uploadedSegments cache and the checksum matches.
     * It is important to match the checksum as the same segment filename can be used for different
     * segments due to a concurrency issue.
     * @param localFilename filename of segment stored in local filesystem
     * @param checksum checksum of the segment file
     * @return true if file exists in cache and checksum matches.
     */
    public boolean containsFile(String localFilename, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename)
            && segmentsUploadedToRemoteStore.get(localFilename).checksum.equals(checksum);
    }

    /**
     * Upload metadata file
     * @param segmentFiles segment files that are part of the shard at the time of the latest refresh
     * @param segmentInfosSnapshot SegmentInfos bytes to store as part of metadata file
     * @param storeDirectory instance of local directory to temporarily create metadata file before upload
     * @param primaryTerm primary term to be used in the name of metadata file
     * @throws IOException in case of I/O error while uploading the metadata file
     */
    public void uploadMetadata(
        Collection<String> segmentFiles,
        SegmentInfos segmentInfosSnapshot,
        Directory storeDirectory,
        long primaryTerm
    ) throws IOException {
        synchronized (this) {
            String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
                primaryTerm,
                segmentInfosSnapshot.getGeneration(),
                this.commonFilenameSuffix
            );
            IndexOutput indexOutput = storeDirectory.createOutput(metadataFilename, IOContext.DEFAULT);
            Map<String, String> uploadedSegments = new HashMap<>();
            for (String file : segmentFiles) {
                if (segmentsUploadedToRemoteStore.containsKey(file)) {
                    uploadedSegments.put(file, segmentsUploadedToRemoteStore.get(file).toString());
                } else {
                    throw new NoSuchFileException(file);
                }
            }

            ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
            segmentInfosSnapshot.write(new ByteBuffersIndexOutput(byteBuffersIndexOutput, "", ""));
            byte[] byteArray = byteBuffersIndexOutput.toArrayCopy();

            metadataStreamWrapper.writeStream(
                indexOutput,
                new RemoteSegmentMetadata(RemoteSegmentMetadata.fromMapOfStrings(uploadedSegments), byteArray, segmentInfosSnapshot.getGeneration())
            );
            indexOutput.close();
            storeDirectory.sync(Collections.singleton(metadataFilename));
            remoteMetadataDirectory.copyFrom(storeDirectory, metadataFilename, metadataFilename, IOContext.DEFAULT);
            storeDirectory.deleteFile(metadataFilename);
        }
    }

    private String getChecksumOfLocalFile(Directory directory, String file) throws IOException {
        try (IndexInput indexInput = directory.openInput(file, IOContext.DEFAULT)) {
            return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }

    private String getExistingRemoteFilename(String localFilename) {
        if (segmentsUploadedToRemoteStore.containsKey(localFilename)) {
            return segmentsUploadedToRemoteStore.get(localFilename).uploadedFilename;
        } else {
            return null;
        }
    }

    private String getNewRemoteSegmentFilename(String localFilename) {
        return localFilename + SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
    }

    private String getLocalSegmentFilename(String remoteFilename) {
        return remoteFilename.split(SEGMENT_NAME_UUID_SEPARATOR)[0];
    }

    // Visible for testing
    public Map<String, UploadedSegmentMetadata> getSegmentsUploadedToRemoteStore() {
        return Collections.unmodifiableMap(this.segmentsUploadedToRemoteStore);
    }

    public Map<String, UploadedSegmentMetadata> getSegmentsUploadedToRemoteStore(long primaryTerm, long generation) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);

        Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(
            readMetadataFile(metadataFile).getMetadata()
        );
        return Collections.unmodifiableMap(segmentsUploadedToRemoteStore);
    }

    /**
     * Delete stale segment and metadata files
     * One metadata file is kept per commit (refresh updates the same file). To read segments uploaded to remote store,
     * we just need to read the latest metadata file. All the stale metadata files can be safely deleted.
     * @param lastNMetadataFilesToKeep number of metadata files to keep
     * @throws IOException in case of I/O error while reading from / writing to remote segment store
     */
    public void deleteStaleSegments(int lastNMetadataFilesToKeep) throws IOException {
        Collection<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefix(MetadataFilenameUtils.METADATA_PREFIX);
        List<String> sortedMetadataFileList = metadataFiles.stream().sorted(METADATA_FILENAME_COMPARATOR).collect(Collectors.toList());
        if (sortedMetadataFileList.size() <= lastNMetadataFilesToKeep) {
            logger.info(
                "Number of commits in remote segment store={}, lastNMetadataFilesToKeep={}",
                sortedMetadataFileList.size(),
                lastNMetadataFilesToKeep
            );
            return;
        }

        List<String> metadataFilesEligibleToDelete = sortedMetadataFileList.subList(
            0,
            sortedMetadataFileList.size() - lastNMetadataFilesToKeep
        );
        List<String> metadataFilesToBeDeleted = metadataFilesEligibleToDelete.stream().filter(metadataFile -> {
            try {
                // TODO: add snapshot interop feature flag here as that will be the first feature to use lock
                // manager.
                boolean lockManagerEnabled = false;
                if (!lockManagerEnabled) {
                    return true;
                }
                return !isLockAcquired(
                    MetadataFilenameUtils.getPrimaryTerm(metadataFile.split(MetadataFilenameUtils.SEPARATOR)),
                    MetadataFilenameUtils.getGeneration(metadataFile.split(MetadataFilenameUtils.SEPARATOR))
                );
            } catch (IOException e) {
                logger.error(
                    "skipping metadata file ("
                        + metadataFile
                        + ") deletion for this run,"
                        + " as checking lock for metadata is failing with error: "
                        + e
                );
                return false;
            }
        }).collect(Collectors.toList());

        sortedMetadataFileList.removeAll(metadataFilesToBeDeleted);

        Map<String, UploadedSegmentMetadata> activeSegmentFilesMetadataMap = new HashMap<>();
        Set<String> activeSegmentRemoteFilenames = new HashSet<>();
        for (String metadataFile : sortedMetadataFileList) {
            Map<String, UploadedSegmentMetadata> segmentMetadataMap = readMetadataFile(metadataFile).getMetadata();
            activeSegmentFilesMetadataMap.putAll(segmentMetadataMap);
            activeSegmentRemoteFilenames.addAll(
                segmentMetadataMap.values().stream().map(metadata -> metadata.uploadedFilename).collect(Collectors.toSet())
            );
        }
        for (String metadataFile : metadataFilesToBeDeleted) {
            Map<String, UploadedSegmentMetadata> staleSegmentFilesMetadataMap = readMetadataFile(metadataFile).getMetadata();
            Set<String> staleSegmentRemoteFilenames = staleSegmentFilesMetadataMap.values()
                .stream()
                .map(metadata -> metadata.uploadedFilename)
                .collect(Collectors.toSet());
            AtomicBoolean deletionSuccessful = new AtomicBoolean(true);
            staleSegmentRemoteFilenames.stream().filter(file -> !activeSegmentRemoteFilenames.contains(file)).forEach(file -> {
                try {
                    remoteDataDirectory.deleteFile(file);
                    if (!activeSegmentFilesMetadataMap.containsKey(getLocalSegmentFilename(file))) {
                        segmentsUploadedToRemoteStore.remove(getLocalSegmentFilename(file));
                    }
                } catch (NoSuchFileException e) {
                    logger.info("Segment file {} corresponding to metadata file {} does not exist in remote", file, metadataFile);
                } catch (IOException e) {
                    deletionSuccessful.set(false);
                    logger.info(
                        "Exception while deleting segment file {} corresponding to metadata file {}. Deletion will be re-tried",
                        file,
                        metadataFile
                    );
                }
            });
            if (deletionSuccessful.get()) {
                logger.info("Deleting stale metadata file {} from remote segment store", metadataFile);
                remoteMetadataDirectory.deleteFile(metadataFile);
            }
        }
    }
}

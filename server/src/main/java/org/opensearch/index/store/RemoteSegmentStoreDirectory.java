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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.UUIDs;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * remote segment store also keeps track of refresh and commit checkpoints in a separate path which is handled by
 * another instance of {@code RemoteDirectory}.
 * @opensearch.internal
 */
public final class RemoteSegmentStoreDirectory extends FilterDirectory {
    /**
     * Each segment file is uploaded with unique suffix.
     * For example, _0.cfe in local filesystem will be uploaded to remote segment store as _0.cfe__gX7bNIIBrs0AUNsR2yEG
     */
    public static final String SEGMENT_NAME_UUID_SEPARATOR = "__";

    private static final MetadataFilenameUtils.MetadataFilenameComparator metadataFilenameComparator = new MetadataFilenameUtils.MetadataFilenameComparator();

    /**
     * remoteDataDirectory is used to store segment files at path: cluster_UUID/index_UUID/shardId/segments/data
     */
    private final RemoteDirectory remoteDataDirectory;
    /**
     * remoteMetadataDirectory is used to store metadata files at path: cluster_UUID/index_UUID/shardId/segments/metadata
     */
    private final RemoteDirectory remoteMetadataDirectory;

    /**
     * To prevent explosion of refresh metadata files, we replace refresh files for the given primary term and generation
     * This is achieved by uploading refresh metadata file with the same UUID suffix as that of last commit metadata file.
     */
    private String refreshMetadataFileUniqueSuffix;

    /**
     * Keeps track of local segment filename to uploaded filename along with other attributes like checksum.
     * This map acts as a cache layer for uploaded segment filenames which helps avoid calling listAll() each time.
     * It is important to initialize this map on creation of RemoteSegmentStoreDirectory and update it on each upload and delete.
     */
    private Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    private static final Logger logger = LogManager.getLogger(RemoteSegmentStoreDirectory.class);

    public RemoteSegmentStoreDirectory(RemoteDirectory remoteDataDirectory, RemoteDirectory remoteMetadataDirectory) throws IOException {
        super(remoteDataDirectory);
        this.remoteDataDirectory = remoteDataDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        init();
    }

    /**
     * Initializes the cache which keeps track of all the segment files uploaded to the remote segment store.
     * As this cache is specific to an instance of RemoteSegmentStoreDirectory, it is possible that cache becomes stale
     * if another instance of RemoteSegmentStoreDirectory is used to upload/delete segment files.
     * It is caller's responsibility to call init() again to ensure that cache is properly updated.
     * @throws IOException if there were any failures in reading the metadata file
     */
    public void init() throws IOException {
        this.refreshMetadataFileUniqueSuffix = UUIDs.base64UUID();
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(readLatestMetadataFile());
    }

    /**
     * Read latest metadata file to get the list of segments uploaded to the remote segment store.
     * We upload a separate metadata file per commit, commit_metadata__PrimaryTerm__Generation__UUID
     * We also keep a metadata file per refresh, but it is not unique per refresh. Refresh metadata file is unique for a given commit.
     * The format of refresh metadata filename is: refresh_metadata__PrimaryTerm__Generation__LastCommitUUID
     * Commit metadata files keep track of all the segments of the given shard that are part of the commit.
     * Refresh metadata files keep track of segments that were created since the last commit.
     * In order to get the list of segment files uploaded to the remote segment store, we need to read the latest commit metadata file
     * and corresponding refresh metadata file.
     * Each metadata file contains a map where
     *      Key is - Segment local filename and
     *      Value is - local filename::uploaded filename::checksum
     * @return Map of segment filename to uploaded filename with checksum
     * @throws IOException if there were any failures in reading the metadata file
     */
    private Map<String, UploadedSegmentMetadata> readLatestMetadataFile() throws IOException {
        Map<String, UploadedSegmentMetadata> segmentMetadataMap = new HashMap<>();

        Collection<String> commitMetadataFiles = remoteMetadataDirectory.listFilesByPrefix(MetadataFilenameUtils.COMMIT_METADATA_PREFIX);
        Optional<String> latestCommitMetadataFile = commitMetadataFiles.stream().max(metadataFilenameComparator);

        if (latestCommitMetadataFile.isPresent()) {
            logger.info("Reading latest commit Metadata file {}", latestCommitMetadataFile.get());
            segmentMetadataMap = readMetadataFile(latestCommitMetadataFile.get());
        } else {
            logger.info("No commit metadata file found");
        }

        Collection<String> refreshMetadataFiles = remoteMetadataDirectory.listFilesByPrefix(MetadataFilenameUtils.REFRESH_METADATA_PREFIX);
        Optional<String> latestRefreshMetadataFile = refreshMetadataFiles.stream()
            .filter(
                file -> latestCommitMetadataFile.map(
                    s -> metadataFilenameComparator.comparePrimaryTermGeneration(file, s) == 0
                        && MetadataFilenameUtils.getUuid(file).equals(MetadataFilenameUtils.getUuid(s))
                ).orElse(true)
            )
            .max(metadataFilenameComparator);

        if (latestRefreshMetadataFile.isPresent()) {
            logger.info("Reading latest refresh metadata file {}", latestRefreshMetadataFile.get());
            segmentMetadataMap.putAll(readMetadataFile(latestRefreshMetadataFile.get()));
        }
        return segmentMetadataMap;
    }

    private Map<String, UploadedSegmentMetadata> readMetadataFile(String metadataFilename) throws IOException {
        try (IndexInput indexInput = remoteMetadataDirectory.openInput(metadataFilename, IOContext.DEFAULT)) {
            Map<String, String> segmentMetadata = indexInput.readMapOfStrings();
            return segmentMetadata.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> UploadedSegmentMetadata.fromString(entry.getValue())));
        }
    }

    /**
     * Metadata of a segment that is uploaded to remote segment store.
     */
    static class UploadedSegmentMetadata {
        private static final String SEPARATOR = "::";
        private final String originalFilename;
        private final String uploadedFilename;
        private final String checksum;

        UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum) {
            this.originalFilename = originalFilename;
            this.uploadedFilename = uploadedFilename;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return String.join(SEPARATOR, originalFilename, uploadedFilename, checksum);
        }

        public static UploadedSegmentMetadata fromString(String uploadedFilename) {
            String[] values = uploadedFilename.split(SEPARATOR);
            return new UploadedSegmentMetadata(values[0], values[1], values[2]);
        }
    }

    /**
     * Contains utility methods that provide various parts of metadata filename along with comparator
     * Each metadata filename is of format: PREFIX__PrimaryTerm__Generation__UUID
     */
    static class MetadataFilenameUtils {
        public static final String SEPARATOR = "__";
        public static final String COMMIT_METADATA_PREFIX = "commit_metadata";
        public static final String REFRESH_METADATA_PREFIX = "refresh_metadata";

        /**
         * Comparator to sort the metadata filenames. The order of sorting is: Primary Term, Generation, UUID
         * Even though UUID sort does not provide any info on recency, it provides a consistent way to sort the filenames.
         */
        static class MetadataFilenameComparator implements Comparator<String> {
            @Override
            public int compare(String first, String second) {
                if (!first.split(SEPARATOR)[0].equals(second.split(SEPARATOR)[0])) {
                    return first.split(SEPARATOR)[0].compareTo(second.split(SEPARATOR)[0]);
                }
                int fixedSuffixComparison = comparePrimaryTermGeneration(first, second);
                if (fixedSuffixComparison == 0) {
                    return getUuid(first).compareTo(getUuid(second));
                } else {
                    return fixedSuffixComparison;
                }
            }

            public int comparePrimaryTermGeneration(String first, String second) {
                long firstPrimaryTerm = getPrimaryTerm(first);
                long secondPrimaryTerm = getPrimaryTerm(second);
                if (firstPrimaryTerm != secondPrimaryTerm) {
                    return firstPrimaryTerm > secondPrimaryTerm ? 1 : -1;
                } else {
                    long firstGeneration = getGeneration(first);
                    long secondGeneration = getGeneration(second);
                    if (firstGeneration != secondGeneration) {
                        return firstGeneration > secondGeneration ? 1 : -1;
                    } else {
                        return 0;
                    }
                }
            }
        }

        public static String getMetadataFilename(String prefix, long primaryTerm, long generation) {
            return getMetadataFilename(prefix, primaryTerm, generation, UUIDs.base64UUID());
        }

        public static String getMetadataFilename(String prefix, long primaryTerm, long generation, String uuid) {
            return String.join(SEPARATOR, prefix, Long.toString(primaryTerm), Long.toString(generation, Character.MAX_RADIX), uuid);
        }

        public static long getPrimaryTerm(String filename) {
            return Long.parseLong(filename.split(SEPARATOR)[1]);
        }

        public static long getGeneration(String filename) {
            return Long.parseLong(filename.split(SEPARATOR)[2], Character.MAX_RADIX);
        }

        public static String getUuid(String filename) {
            return filename.split(SEPARATOR)[3];
        }
    }

    /**
     * Returns list of all the segment files uploaded to remote segment store till the last refresh checkpoint.
     * Any segment file that is uploaded without corresponding refresh/commit file will not be visible as part of listAll().
     * We chose not to return cache entries for listAll as cache can have entries for stale segments as well.
     * Even if we plan to delete stale segments from remote segment store, it will be a periodic operation.
     * @return segment filenames stored in remote segment store
     * @throws IOException if there were any failures in reading the metadata file
     */
    @Override
    public String[] listAll() throws IOException {
        return readLatestMetadataFile().keySet().toArray(new String[0]);
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
     * Copies an existing src file from directory from to a non-existent file dest in this directory.
     * Once the segment is uploaded to remote segment store, update the cache accordingly.
     */
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        String remoteFilename = getNewRemoteSegmentFilename(dest);
        remoteDataDirectory.copyFrom(from, src, remoteFilename, context);
        String checksum = getChecksumOfLocalFile(from, src);
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(src, remoteFilename, checksum);
        segmentsUploadedToRemoteStore.put(src, metadata);
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
     * Upload commit metadata file
     * @param committedFiles segment files that are part of the latest segments_N file
     * @param storeDirectory instance of local directory to temporarily create metadata file before upload
     * @param primaryTerm primary term to be used in the name of metadata file
     * @param generation commit generation
     * @throws IOException in case of I/O error while uploading the metadata file
     */
    public void uploadCommitMetadata(Collection<String> committedFiles, Directory storeDirectory, long primaryTerm, long generation)
        throws IOException {
        String commitFilename = MetadataFilenameUtils.getMetadataFilename(
            MetadataFilenameUtils.COMMIT_METADATA_PREFIX,
            primaryTerm,
            generation
        );
        uploadMetadataFile(committedFiles, storeDirectory, commitFilename);
        this.refreshMetadataFileUniqueSuffix = UUIDs.base64UUID();
    }

    /**
     * Upload commit metadata file
     * @param refreshedFiles segment files that are created since last commit and part of the latest refresh
     * @param storeDirectory instance of local directory to temporarily create metadata file before upload
     * @param primaryTerm primary term to be used in the name of metadata file
     * @param generation commit generation
     * @throws IOException in case of I/O error while uploading the metadata file
     */
    public void uploadRefreshMetadata(Collection<String> refreshedFiles, String lastCommitFile, Directory storeDirectory, long primaryTerm, long generation)
        throws IOException {
        String refreshMetadataUUID;
        if(lastCommitFile != null && containsFile(lastCommitFile, getChecksumOfLocalFile(storeDirectory, lastCommitFile))) {
            refreshMetadataUUID = MetadataFilenameUtils.getUuid(segmentsUploadedToRemoteStore.get(lastCommitFile).uploadedFilename);
        } else {
            if(this.refreshMetadataFileUniqueSuffix == null) {
                this.refreshMetadataFileUniqueSuffix = UUIDs.base64UUID();
            }
            refreshMetadataUUID = this.refreshMetadataFileUniqueSuffix;
        }
        String refreshFilename = MetadataFilenameUtils.getMetadataFilename(
            MetadataFilenameUtils.REFRESH_METADATA_PREFIX,
            primaryTerm,
            generation,
            refreshMetadataUUID
        );
        uploadMetadataFile(refreshedFiles, storeDirectory, refreshFilename);
    }

    private void uploadMetadataFile(Collection<String> files, Directory storeDirectory, String filename) throws IOException {
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        Map<String, String> uploadedSegments = new HashMap<>();
        for(String file: files) {
            if(segmentsUploadedToRemoteStore.containsKey(file)) {
                uploadedSegments.put(file, segmentsUploadedToRemoteStore.get(file).toString());
            } else {
                throw new NoSuchFileException(file);
            }
        }
        indexOutput.writeMapOfStrings(uploadedSegments);
        indexOutput.close();
        storeDirectory.sync(Collections.singleton(filename));
        remoteMetadataDirectory.copyFrom(storeDirectory, filename, filename, IOContext.DEFAULT);
        storeDirectory.deleteFile(filename);
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
    Map<String, UploadedSegmentMetadata> getSegmentsUploadedToRemoteStore() {
        return this.segmentsUploadedToRemoteStore;
    }

    public void deleteStaleCommits(int lastNCommitsToKeep) throws IOException {
        Collection<String> commitMappingFiles = remoteMetadataDirectory.listFilesByPrefix(MetadataFilenameUtils.COMMIT_METADATA_PREFIX);
        List<String> sortedMappingFileList = commitMappingFiles.stream().sorted(new MetadataFilenameUtils.MetadataFilenameComparator()).collect(Collectors.toList());
        if(sortedMappingFileList.size() <= lastNCommitsToKeep) {
            logger.info("Number of commits in remote segment store={}, lastNCommitsToKeep={}", sortedMappingFileList.size(), lastNCommitsToKeep);
            return;
        }
        List<String> latestNCommitFiles = sortedMappingFileList.subList(sortedMappingFileList.size() - lastNCommitsToKeep, sortedMappingFileList.size());
        Map<String, UploadedSegmentMetadata> activeSegmentFilesMetadataMap = new HashMap<>();
        for(String commitFile: latestNCommitFiles) {
            activeSegmentFilesMetadataMap.putAll(readMetadataFile(commitFile));
        }
        Set<String> activeSegmentRemoteFilenames = activeSegmentFilesMetadataMap.values().stream().map(metadata -> metadata.uploadedFilename).collect(Collectors.toSet());
        for(String commitFile: sortedMappingFileList.subList(0, sortedMappingFileList.size() - lastNCommitsToKeep)) {
            Map<String, UploadedSegmentMetadata> staleSegmentFilesMetadataMap = readMetadataFile(commitFile);
            Set<String> staleSegmentRemoteFilenames = staleSegmentFilesMetadataMap.values().stream().map(metadata -> metadata.uploadedFilename).collect(Collectors.toSet());
            AtomicBoolean deletionSuccessful = new AtomicBoolean(true);
            staleSegmentRemoteFilenames.stream().filter(file -> !activeSegmentRemoteFilenames.contains(file)).forEach(file -> {
                try {
                    remoteDataDirectory.deleteFile(file);
                    if(!activeSegmentFilesMetadataMap.containsKey(getLocalSegmentFilename(file))) {
                        segmentsUploadedToRemoteStore.remove(getLocalSegmentFilename(file));
                    }
                } catch (IOException e) {
                    deletionSuccessful.set(false);
                    logger.info("Exception while deleting segment files related to commit file {}. Deletion will be re-tried", commitFile);
                }
            });
            if(deletionSuccessful.get()) {
                logger.info("Deleting stale commit metadata file {} from remote segment store", commitFile);
                remoteMetadataDirectory.deleteFile(commitFile);
                remoteMetadataDirectory.deleteFile(MetadataFilenameUtils.REFRESH_METADATA_PREFIX + commitFile.substring(MetadataFilenameUtils.COMMIT_METADATA_PREFIX.length()));
            }
        }
    }
}

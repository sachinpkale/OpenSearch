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

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to safely share {@link OpenSearchDirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 *
 * @see SearcherManager
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "reference counting is required here")
class OpenSearchReaderManager extends ReferenceManager<OpenSearchDirectoryReader> {
    /**
     * Creates and returns a new OpenSearchReaderManager from the given
     * already-opened {@link OpenSearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader            the directoryReader to use for future reopens
     */
    OpenSearchReaderManager(OpenSearchDirectoryReader reader) {
        this.current = reader;
    }

    @Override
    protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        final OpenSearchDirectoryReader reader = (OpenSearchDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);
        if (reader != null) {
            List<String> segments = new ArrayList<>();
            segments.add("ABCDE");
            segments.addAll(((StandardDirectoryReader)reader.getDelegate()).getSegmentInfos().files(true));
            reader.directory().sync(segments);

            SegmentInfos segmentInfos = ((StandardDirectoryReader)reader.getDelegate()).getSegmentInfos();



//            FilterDirectory remoteStoreDirectory = (FilterDirectory) reader.directory();
//            FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
//            final OpenSearchDirectory openSearchDirectory = (org.opensearch.index.store.OpenSearchDirectory) byteSizeCachingStoreDirectory.getDelegate();
//
//            Directory cacheDirectory = openSearchDirectory.getCache();
//            RemoteSegmentStoreDirectory storageDirectory = (RemoteSegmentStoreDirectory) openSearchDirectory.getStorage();
//            try {
//                OpenSearchDirectorySyncManager.syncCacheFilesToStorage(((StandardDirectoryReader)reader.getDelegate()).getSegmentInfos(), cacheDirectory, storageDirectory);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
        }
        return reader;
    }

    @Override
    protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchDirectoryReader reference) {
        return reference.getRefCount();
    }
}

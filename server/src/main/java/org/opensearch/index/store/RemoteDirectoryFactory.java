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

import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.RemoteDirectory;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;

public class RemoteDirectoryFactory implements IndexStorePlugin.RemoteDirectoryFactory {

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path, RepositoriesService repositoriesService) throws IOException {
        Repository repository = repositoriesService.repository("dragon-stone");
        BlobPath blobPath = new BlobPath();
        blobPath = blobPath.add(indexSettings.getIndex().getName());
        blobPath = blobPath.add(String.valueOf(path.getShardId()));
        BlobContainer blobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(blobPath);
        return new RemoteDirectory(blobContainer);
    }
}

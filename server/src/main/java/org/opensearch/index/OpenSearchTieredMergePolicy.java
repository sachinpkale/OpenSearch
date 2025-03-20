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

package org.opensearch.index;

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;

/**
 * Wrapper around {@link TieredMergePolicy} which doesn't respect
 * {@link TieredMergePolicy#setMaxMergedSegmentMB(double)} on forced merges, but DOES respect it on only_expunge_deletes.
 * See https://issues.apache.org/jira/browse/LUCENE-7976.
 *
 * @opensearch.internal
 */
final class OpenSearchTieredMergePolicy extends FilterMergePolicy {

    final TieredMergePolicy regularMergePolicy;
    final TieredMergePolicy forcedMergePolicy;
    final Logger logger;

    OpenSearchTieredMergePolicy() {
        super(new TieredMergePolicy());
        regularMergePolicy = (TieredMergePolicy) in;
        forcedMergePolicy = new TieredMergePolicy();
        forcedMergePolicy.setMaxMergedSegmentMB(Double.POSITIVE_INFINITY); // unlimited
        logger = Loggers.getLogger(getClass(), new ShardId("dummy1", "dummy2", 0));
    }

    public boolean isGreater(SegmentCommitInfo s1, SegmentCommitInfo s2) {
        long s1Long = Long.parseLong(s1.info.name.substring(1), Character.MAX_RADIX);
        long s2Long = Long.parseLong(s2.info.name.substring(1), Character.MAX_RADIX);

        return s1Long > s2Long;
    }

    @Override
    public MergeSpecification findFullFlushMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        MergeSpecification mergeSpecification = super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
        if (mergeSpecification == null) {
            return null;
        }
        List<OneMerge> merges = mergeSpecification.merges;
        OneMerge merge = merges.removeLast();
        List<SegmentCommitInfo> segmentList = new ArrayList<>(merge.segments);
        SegmentCommitInfo lastestSegmentCommitInfo = null;
        for(SegmentCommitInfo segmentCommitInfo: segmentList) {
            if (lastestSegmentCommitInfo == null) {
                lastestSegmentCommitInfo = segmentCommitInfo;
            } else {
                if (isGreater(segmentCommitInfo, lastestSegmentCommitInfo)) {
                    lastestSegmentCommitInfo = segmentCommitInfo;
                }
            }
        }
        segmentList.remove(lastestSegmentCommitInfo);
        logger.error("Removing: {}", lastestSegmentCommitInfo);
        OneMerge modified = new OneMerge(segmentList);
        merges.add(modified);
        return mergeSpecification;
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos infos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext
    ) throws IOException {
        return forcedMergePolicy.findForcedMerges(infos, maxSegmentCount, segmentsToMerge, mergeContext);
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, MergeContext mergeContext) throws IOException {
        return regularMergePolicy.findForcedDeletesMerges(infos, mergeContext);
    }

    public void setForceMergeDeletesPctAllowed(double forceMergeDeletesPctAllowed) {
        regularMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
        forcedMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
    }

    public double getForceMergeDeletesPctAllowed() {
        return regularMergePolicy.getForceMergeDeletesPctAllowed();
    }

    public void setFloorSegmentMB(double mbFrac) {
        regularMergePolicy.setFloorSegmentMB(mbFrac);
        forcedMergePolicy.setFloorSegmentMB(mbFrac);
    }

    public double getFloorSegmentMB() {
        return regularMergePolicy.getFloorSegmentMB();
    }

    public void setMaxMergeAtOnce(int maxMergeAtOnce) {
        regularMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        forcedMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
    }

    public int getMaxMergeAtOnce() {
        return regularMergePolicy.getMaxMergeAtOnce();
    }

    // only setter that must NOT delegate to the forced merge policy
    public void setMaxMergedSegmentMB(double mbFrac) {
        regularMergePolicy.setMaxMergedSegmentMB(mbFrac);
    }

    public double getMaxMergedSegmentMB() {
        return regularMergePolicy.getMaxMergedSegmentMB();
    }

    public void setSegmentsPerTier(double segmentsPerTier) {
        regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
        forcedMergePolicy.setSegmentsPerTier(segmentsPerTier);
    }

    public double getSegmentsPerTier() {
        return regularMergePolicy.getSegmentsPerTier();
    }

    public void setDeletesPctAllowed(double deletesPctAllowed) {
        regularMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
        forcedMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
    }

    public double getDeletesPctAllowed() {
        return regularMergePolicy.getDeletesPctAllowed();
    }
}

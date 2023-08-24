/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class MockNRTReplicationEngine extends NRTReplicationEngine {

    protected volatile Long latestReceivedCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;

    List<Tuple<Long, Consumer<Boolean>>> listeners = new ArrayList<>();

    public MockNRTReplicationEngine(EngineConfig config) {
        super(config);
    }

    public synchronized void updateSegments(final SegmentInfos infos) throws IOException {
        super.updateSegments(infos);
        checkAndFireListeners();
    }

    @Override
    public void awaitCurrent(Consumer<Boolean> listener) {
        final long processedLocalCheckpoint = getProcessedLocalCheckpoint();
        final long maxSeqNo = getLocalCheckpointTracker().getMaxSeqNo();
//        logger.info("awaitCurrent Processed {} - max {}", processedLocalCheckpoint, maxSeqNo);
        if (processedLocalCheckpoint == maxSeqNo) {
            listener.accept(true);
        } else {
//            logger.info("Added listener for max {}", maxSeqNo);
            listeners.add(new Tuple<>(maxSeqNo, listener));
        }
    }

    private void checkAndFireListeners() {
        final long processedLocalCheckpoint = getProcessedLocalCheckpoint();
        List<Tuple> listenersToClear = new ArrayList<>();
        for (Tuple<Long, Consumer<Boolean>> listener : listeners) {
            if (listener.v1() <= processedLocalCheckpoint) {
                listener.v2().accept(true);
//                logger.info("Firing listener on {} {}", listener.v1(), processedLocalCheckpoint);
                listenersToClear.add(listener);
            } else {
                logger.info("still waiting listener on {} {}", listener.v1(), processedLocalCheckpoint);
            }
        }
        listeners.removeAll(listenersToClear);
    }

    @Override
    public void updateLatestReceivedCheckpoint(Long cp) {
        this.latestReceivedCheckpoint = cp;
    }

    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        // wait until we are caught up to return this.
        while (latestReceivedCheckpoint != -1 && latestReceivedCheckpoint != getLatestSegmentInfos().getVersion()) {
//            logger.info("Received {} latest {}", latestReceivedCheckpoint, getLatestSegmentInfos().getVersion());
        }
        return super.acquireLastIndexCommit(flushFirst);
    }
}

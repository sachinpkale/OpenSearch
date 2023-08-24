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
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
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

    final List<Tuple<Long, Consumer<Boolean>>> listeners = new ArrayList<>();
    final List<Tuple<Long, Consumer<Boolean>>> checkpointListeners = new ArrayList<>();

    public MockNRTReplicationEngine(EngineConfig config) {
        super(config);
    }

    public synchronized void updateSegments(final SegmentInfos infos) throws IOException {
        super.updateSegments(infos);
        synchronized (listeners) {
            fireListeners(getProcessedLocalCheckpoint(), listeners);
        }
    }

    @Override
    public void awaitCurrent(Consumer<Boolean> listener) {
        synchronized (listeners) {
            final long processedLocalCheckpoint = getProcessedLocalCheckpoint();
            final long maxSeqNo = getLocalCheckpointTracker().getMaxSeqNo();
            if (processedLocalCheckpoint == maxSeqNo) {
                listener.accept(true);
            } else {
                listeners.add(new Tuple<>(maxSeqNo, listener));
            }
        }
    }

    private void awaitCheckpointUpdate(Consumer<Boolean> listener) {
        synchronized (checkpointListeners) {
            final long localVersion = getLatestSegmentInfos().getVersion();
            if (latestReceivedCheckpoint == localVersion) {
                listener.accept(true);
            } else {
                checkpointListeners.add(new Tuple<>(latestReceivedCheckpoint, listener));
            }
        }
    }

    @Override
    public void updateLatestReceivedCheckpoint(Long cp) {
        this.latestReceivedCheckpoint = cp;
        synchronized (checkpointListeners) {
            fireListeners(getLatestSegmentInfos().getVersion(), checkpointListeners);
        }
    }

    private void fireListeners(long localVersion, List<Tuple<Long, Consumer<Boolean>>> checkpointListeners) {
        List<Tuple> listenersToClear = new ArrayList<>();
        for (Tuple<Long, Consumer<Boolean>> listener : listeners) {
            if (listener.v1() <= localVersion) {
                listener.v2().accept(true);
                listenersToClear.add(listener);
            } else {
                logger.info("still waiting listener on {} {}", listener.v1(), localVersion);
            }
        }
        checkpointListeners.removeAll(listenersToClear);
    }

    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        // wait until we are caught up to return this.
        if (flushFirst) {
           awaitCheckpointUpdate((b) -> super.acquireLastIndexCommit(flushFirst));
        }
        return super.acquireLastIndexCommit(flushFirst);
    }
}

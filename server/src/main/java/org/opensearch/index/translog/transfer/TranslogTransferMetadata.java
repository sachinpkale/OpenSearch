/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.SetOnce;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * The metadata associated with every transfer {@link TransferSnapshot}. The metadata is uploaded at the end of the
 * tranlog and generational checkpoint uploads to mark the latest generation and the translog/checkpoint files that are
 * still referenced by the last checkpoint.
 *
 * @opensearch.internal
 */
public class TranslogTransferMetadata {

    private final long primaryTerm;

    private final long generation;

    private final long minTranslogGeneration;

    private final long timeStamp;

    private final int count;

    private final SetOnce<Map<String, String>> generationToPrimaryTermMapper = new SetOnce<>();

    private static final String METADATA_SEPARATOR = "__";

    private static final int BUFFER_SIZE = 4096;

    private static final int CURRENT_VERSION = 1;

    private static final String METADATA_CODEC = "md";

    public TranslogTransferMetadata(long primaryTerm, long generation, long minTranslogGeneration, int count) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.timeStamp = System.currentTimeMillis();
        this.count = count;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getGeneration() {
        return generation;
    }

    public long getMinTranslogGeneration() {
        return minTranslogGeneration;
    }

    public int getCount() {
        return count;
    }

    public void setGenerationToPrimaryTermMapper(Map<String, String> generationToPrimaryTermMap) {
        generationToPrimaryTermMapper.set(generationToPrimaryTermMap);
    }

    public String getFileName() {
        return String.join(
            METADATA_SEPARATOR,
            Arrays.asList(String.valueOf(primaryTerm), String.valueOf(generation), String.valueOf(timeStamp))
        );
    }

    public byte[] createMetadataBytes() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "translog transfer metadata " + primaryTerm,
                    getFileName(),
                    output,
                    BUFFER_SIZE
                )
            ) {
                CodecUtil.writeHeader(indexOutput, METADATA_CODEC, CURRENT_VERSION);
                write(indexOutput);
                CodecUtil.writeFooter(indexOutput);
            }
            return BytesReference.toBytes(output.bytes());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, generation, timeStamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TranslogTransferMetadata other = (TranslogTransferMetadata) o;
        return Objects.equals(this.primaryTerm, other.primaryTerm)
            && Objects.equals(this.generation, other.generation)
            && Objects.equals(this.timeStamp, other.timeStamp);
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(primaryTerm);
        out.writeLong(generation);
        out.writeLong(minTranslogGeneration);
        out.writeLong(timeStamp);
        out.writeMapOfStrings(generationToPrimaryTermMapper.get());
    }
}

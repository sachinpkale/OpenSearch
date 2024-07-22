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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains metadata about remote store pinned timestamp
 *
 * @opensearch.internal
 */
public class PinnedTimestampMetadata extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "pinned_timestamps";
    private static final ParseField PINNED_TIMESTAMPS = new ParseField("pinned_timestamps");

    private final Map<Long, List<String>> pinnedTimestampMap;

    public PinnedTimestampMetadata(Map<Long, List<String>> pinnedTimestampMap) {
        this.pinnedTimestampMap = Collections.unmodifiableMap(pinnedTimestampMap);
    }

    public PinnedTimestampMetadata(StreamInput in) throws IOException {
        this.pinnedTimestampMap = in.readMapOfLists(StreamInput::readLong, StreamInput::readString);
    }

    public PinnedTimestampMetadata withUpdatedTimestamp(long timestamp, String acquirer) {
        Map<Long, List<String>> updatedPinnedTimestampMap = new HashMap<>(pinnedTimestampMap);
        if (updatedPinnedTimestampMap.containsKey(timestamp) == false) {
           updatedPinnedTimestampMap.put(timestamp, new ArrayList<>());
        }
        updatedPinnedTimestampMap.get(timestamp).add(acquirer);
        return new PinnedTimestampMetadata(updatedPinnedTimestampMap);
    }

    public Map<Long, List<String>> pinnedTimestampMap() {
        return this.pinnedTimestampMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PinnedTimestampMetadata that = (PinnedTimestampMetadata) o;

        return pinnedTimestampMap.equals(that.pinnedTimestampMap);
    }

    @Override
    public int hashCode() {
        return pinnedTimestampMap.hashCode();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMapOfLists(pinnedTimestampMap, StreamOutput::writeLong, StreamOutput::writeString);
    }

    public static PinnedTimestampMetadata fromXContent(XContentParser parser) throws IOException {
        Map<Long, List<String>> pinnedTimestamps = new HashMap<>();
        XContentParser.Token token;
        String timestamp = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                timestamp = parser.currentName();
            } else {
                List<Object> l = parser.list();
                List<String> acquirers = l == null ? Collections.emptyList() : l.stream().map(Object::toString).collect(Collectors.toList());
                pinnedTimestamps.put(Long.valueOf(timestamp), acquirers);
            }
        }
        return new PinnedTimestampMetadata(pinnedTimestamps);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PINNED_TIMESTAMPS.getPreferredName());
        for (Map.Entry<Long, List<String>> pinnedTimestamp : pinnedTimestampMap.entrySet()) {
            builder.field(String.valueOf(pinnedTimestamp.getKey()), pinnedTimestamp.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;

import java.util.Arrays;
import java.util.Locale;

import static org.opensearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;

/**
 * Settings for remote store
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class RemoteStoreSettings {

    /**
     * Used to specify the default translog buffer interval for remote store backed indexes.
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.buffer_interval",
        IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        IndexSettings.MINIMUM_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls minimum number of metadata files to keep in remote segment store.
     * {@code value < 1} will disable deletion of stale segment metadata files.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING = Setting.intSetting(
        "cluster.remote_store.index.segment_metadata.retention.max_count",
        10,
        -1,
        v -> {
            if (v == 0) {
                throw new IllegalArgumentException(
                    "Value 0 is not allowed for this setting as it would delete all the data from remote segment store"
                );
            }
        },
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<CompatibilityMode> REMOTE_STORE_COMPATIBILITY_MODE_SETTING = new Setting<>(
        "remote_store.compatibility_mode",
        CompatibilityMode.STRICT.name(),
        CompatibilityMode::parseString,
        value -> {
            if (value == CompatibilityMode.MIXED
                && FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING) == false) {
                throw new IllegalArgumentException(
                    " mixed mode is under an experimental feature and can be activated only by enabling "
                        + REMOTE_STORE_MIGRATION_EXPERIMENTAL
                        + " feature flag in the JVM options "
                );
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Direction> MIGRATION_DIRECTION_SETTING = new Setting<>(
        "migration.direction",
        Direction.NONE.name(),
        Direction::parseString,
        value -> {
            if (value != Direction.NONE && FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING) == false) {
                throw new IllegalArgumentException(
                    " migration.direction is under an experimental feature and can be activated only by enabling "
                        + REMOTE_STORE_MIGRATION_EXPERIMENTAL
                        + " feature flag in the JVM options "
                );
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile TimeValue clusterRemoteTranslogBufferInterval;
    private volatile int minRemoteSegmentMetadataFiles;

    public RemoteStoreSettings(Settings settings, ClusterSettings clusterSettings) {
        this.clusterRemoteTranslogBufferInterval = CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
            this::setClusterRemoteTranslogBufferInterval
        );

        minRemoteSegmentMetadataFiles = CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING,
            this::setMinRemoteSegmentMetadataFiles
        );
    }

    // Exclusively for testing, please do not use it elsewhere.
    public TimeValue getClusterRemoteTranslogBufferInterval() {
        return clusterRemoteTranslogBufferInterval;
    }

    private void setClusterRemoteTranslogBufferInterval(TimeValue clusterRemoteTranslogBufferInterval) {
        this.clusterRemoteTranslogBufferInterval = clusterRemoteTranslogBufferInterval;
    }

    private void setMinRemoteSegmentMetadataFiles(int minRemoteSegmentMetadataFiles) {
        this.minRemoteSegmentMetadataFiles = minRemoteSegmentMetadataFiles;
    }

    public int getMinRemoteSegmentMetadataFiles() {
        return this.minRemoteSegmentMetadataFiles;
    }

    /**
     * Node join compatibility mode introduced with remote backed storage.
     *
     * @opensearch.internal
     */
    public enum CompatibilityMode {
        STRICT("strict"),
        MIXED("mixed");

        public final String mode;

        CompatibilityMode(String mode) {
            this.mode = mode;
        }

        public static CompatibilityMode parseString(String compatibilityMode) {
            try {
                return CompatibilityMode.valueOf(compatibilityMode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "["
                        + compatibilityMode
                        + "] compatibility mode is not supported. "
                        + "supported modes are ["
                        + Arrays.toString(CompatibilityMode.values())
                        + "]"
                );
            }
        }
    }

    /**
     * Migration Direction intended for docrep to remote store migration and vice versa
     *
     * @opensearch.internal
     */
    public enum Direction {
        REMOTE_STORE("remote_store"),
        NONE("none"),
        DOCREP("docrep");

        public final String direction;

        Direction(String d) {
            this.direction = d;
        }

        public static Direction parseString(String direction) {
            try {
                return Direction.valueOf(direction.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("[" + direction + "] migration.direction is not supported.");
            }
        }
    }
}

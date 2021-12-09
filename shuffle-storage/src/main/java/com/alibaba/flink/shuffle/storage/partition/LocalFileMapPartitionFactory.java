/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.core.storage.UsableStorageSpaceInfo;
import com.alibaba.flink.shuffle.storage.utils.StorageConfigParseUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/** {@link DataPartitionFactory} of {@link LocalFileMapPartition}. */
@NotThreadSafe
public class LocalFileMapPartitionFactory implements DataPartitionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileMapPartitionFactory.class);

    @GuardedBy("lock in data store")
    protected final Queue<StorageMeta> ssdStorageMetas = new ArrayDeque<>();

    @GuardedBy("lock in data store")
    protected final Queue<StorageMeta> hddStorageMetas = new ArrayDeque<>();

    @GuardedBy("lock in data store")
    protected final UsableStorageSpaceInfo usableSpace = new UsableStorageSpaceInfo(0, 0);

    protected long reservedSpaceBytes;

    protected StorageType preferredStorageType;

    @Override
    public void initialize(Configuration configuration) {
        String directories = configuration.getString(StorageOptions.STORAGE_LOCAL_DATA_DIRS);
        if (directories == null) {
            throw new ConfigurationException(
                    StorageOptions.STORAGE_LOCAL_DATA_DIRS.key() + " is not configured.");
        }

        String diskTypeString = configuration.getString(StorageOptions.STORAGE_PREFERRED_TYPE);
        try {
            preferredStorageType =
                    StorageType.valueOf(CommonUtils.checkNotNull(diskTypeString).trim());
        } catch (Exception exception) {
            throw new ConfigurationException(
                    String.format(
                            "Illegal configured value %s for %s. Must be SSD, HDD or UNKNOWN.",
                            diskTypeString, StorageOptions.STORAGE_PREFERRED_TYPE.key()));
        }

        StorageConfigParseUtils.ParsedPathLists parsedPathLists =
                StorageConfigParseUtils.parseStoragePaths(directories);
        if (parsedPathLists.getAllPaths().isEmpty()) {
            throw new ConfigurationException(
                    String.format(
                            "No valid data dir is configured for %s.",
                            StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
        }

        this.ssdStorageMetas.addAll(
                parsedPathLists.getSsdPaths().stream()
                        .map(storagePath -> new StorageMeta(storagePath, StorageType.SSD))
                        .collect(Collectors.toList()));
        this.hddStorageMetas.addAll(
                parsedPathLists.getHddPaths().stream()
                        .map(storagePath -> new StorageMeta(storagePath, StorageType.HDD))
                        .collect(Collectors.toList()));

        if (ssdStorageMetas.isEmpty() && preferredStorageType == StorageType.SSD) {
            LOG.warn(
                    "No valid data dir of SSD type is configured for {}.",
                    StorageOptions.STORAGE_LOCAL_DATA_DIRS.key());
        }

        if (hddStorageMetas.isEmpty() && preferredStorageType == StorageType.HDD) {
            LOG.warn(
                    "No valid data dir of HDD type is configured for {}.",
                    StorageOptions.STORAGE_LOCAL_DATA_DIRS.key());
        }

        this.reservedSpaceBytes =
                configuration.getMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES).getBytes();
        updateUsableStorageSpace();
    }

    /**
     * Returns the next data path to use for data storage. It serves data path in a simple round
     * robin way. More complicated strategies can be implemented in the future.
     */
    protected StorageMeta getNextDataStorageMeta() {
        switch (preferredStorageType) {
            case SSD:
                {
                    StorageMeta ssdStorageMeta = getStorageMeta(ssdStorageMetas);
                    if (ssdStorageMeta != null
                            && ssdStorageMeta.getUsableStorageSpace() > reservedSpaceBytes) {
                        return ssdStorageMeta;
                    }
                    StorageMeta hddStorageMeta = getStorageMeta(hddStorageMetas);
                    if (hddStorageMeta != null
                            && hddStorageMeta.getUsableStorageSpace() > reservedSpaceBytes) {
                        return hddStorageMeta;
                    }
                    return ssdStorageMeta != null ? ssdStorageMeta : hddStorageMeta;
                }
            case HDD:
                {
                    StorageMeta hddStorageMeta = getStorageMeta(hddStorageMetas);
                    if (hddStorageMeta != null
                            && hddStorageMeta.getUsableStorageSpace() > reservedSpaceBytes) {
                        return hddStorageMeta;
                    }
                    StorageMeta ssdStorageMeta = getStorageMeta(ssdStorageMetas);
                    if (ssdStorageMeta != null
                            && ssdStorageMeta.getUsableStorageSpace() > reservedSpaceBytes) {
                        return ssdStorageMeta;
                    }
                    return hddStorageMeta != null ? hddStorageMeta : ssdStorageMeta;
                }
            default:
                throw new ShuffleException("Illegal preferred storage type.");
        }
    }

    private StorageMeta getStorageMeta(Queue<StorageMeta> storageMetas) {
        int numStorageMetas = storageMetas.size();
        if (numStorageMetas == 0) {
            return null;
        }

        return getStorageMetaInNonEmptyQueue(storageMetas);
    }

    protected StorageMeta getStorageMetaInNonEmptyQueue(Queue<StorageMeta> storageMetas) {
        int numStorageMetas = storageMetas.size();
        StorageMeta maxUsableMeta = null;
        for (int i = 0; i < numStorageMetas; i++) {
            StorageMeta storageMeta = storageMetas.poll();

            if (storageMeta != null) {
                storageMetas.add(storageMeta);
                long usableSpace = storageMeta.getUsableStorageSpace();
                if (maxUsableMeta == null || usableSpace > maxUsableMeta.getUsableStorageSpace()) {
                    maxUsableMeta = storageMeta;
                }

                if (usableSpace > reservedSpaceBytes) {
                    return storageMeta;
                }
            }
        }

        if (storageMetas.remove(maxUsableMeta)) {
            storageMetas.add(maxUsableMeta);
        }
        return maxUsableMeta;
    }

    @Override
    public LocalFileMapPartition createDataPartition(
            PartitionedDataStore dataStore,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID,
            int numMapPartitions,
            int numReducePartitions) {
        CommonUtils.checkArgument(dataPartitionID != null, "Must be not null.");
        CommonUtils.checkArgument(dataPartitionID instanceof MapPartitionID, "Illegal type.");

        MapPartitionID mapPartitionID = (MapPartitionID) dataPartitionID;
        return new LocalFileMapPartition(
                getNextDataStorageMeta(),
                dataStore,
                jobID,
                dataSetID,
                mapPartitionID,
                numReducePartitions);
    }

    @Override
    public LocalFileMapPartition createDataPartition(
            PartitionedDataStore dataStore, DataPartitionMeta partitionMeta) {
        CommonUtils.checkArgument(
                partitionMeta instanceof LocalFileMapPartitionMeta, "Illegal data partition type.");

        return new LocalFileMapPartition(dataStore, (LocalFileMapPartitionMeta) partitionMeta);
    }

    @Override
    public LocalFileMapPartitionMeta recoverDataPartitionMeta(DataInput dataInput)
            throws IOException {
        return LocalFileMapPartitionMeta.readFrom(dataInput);
    }

    /** At the present, only MAP_PARTITION is supported. */
    @Override
    public DataPartition.DataPartitionType getDataPartitionType() {
        return DataPartition.DataPartitionType.MAP_PARTITION;
    }

    @Override
    public void updateUsableStorageSpace() {
        long maxSsdUsableSpaceBytes = 0;
        for (StorageMeta storageMeta : ssdStorageMetas) {
            long usableSpaceBytes = storageMeta.updateUsableStorageSpace();
            if (usableSpaceBytes > maxSsdUsableSpaceBytes) {
                maxSsdUsableSpaceBytes = usableSpaceBytes;
            }
        }
        usableSpace.setSsdUsableSpaceBytes(maxSsdUsableSpaceBytes);

        long maxHddUsableSpaceBytes = 0;
        for (StorageMeta storageMeta : hddStorageMetas) {
            long usableSpaceBytes = storageMeta.updateUsableStorageSpace();
            if (usableSpaceBytes > maxHddUsableSpaceBytes) {
                maxHddUsableSpaceBytes = usableSpaceBytes;
            }
        }
        usableSpace.setHddUsableSpaceBytes(maxHddUsableSpaceBytes);
    }

    @Override
    public UsableStorageSpaceInfo getUsableStorageSpace() {
        return usableSpace;
    }

    @Override
    public boolean isUsableStorageSpaceEnough(
            UsableStorageSpaceInfo usableSpace, long reservedSpaceBytes) {
        return reservedSpaceBytes
                < Math.max(
                        usableSpace.getHddUsableSpaceBytes(), usableSpace.getSsdUsableSpaceBytes());
    }

    @Override
    public boolean useSsdOnly() {
        return false;
    }

    @Override
    public boolean useHddOnly() {
        return false;
    }

    // ---------------------------------------------------------------------------------------------
    // For test
    // ---------------------------------------------------------------------------------------------

    StorageType getPreferredStorageType() {
        return preferredStorageType;
    }

    List<StorageMeta> getSsdStorageMetas() {
        return new ArrayList<>(ssdStorageMetas);
    }

    List<StorageMeta> getHddStorageMetas() {
        return new ArrayList<>(hddStorageMetas);
    }

    void addSsdStorageMeta(StorageMeta storageMeta) {
        ssdStorageMetas.add(storageMeta);
    }

    void addHddStorageMeta(StorageMeta storageMeta) {
        hddStorageMetas.add(storageMeta);
    }
}

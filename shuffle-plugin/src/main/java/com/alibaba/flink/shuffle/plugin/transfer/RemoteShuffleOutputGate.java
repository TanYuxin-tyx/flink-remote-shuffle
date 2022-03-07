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

package com.alibaba.flink.shuffle.plugin.transfer;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.utils.PartitionUtils;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.utils.BufferUtils;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.ReducePartitionWriteClient;
import com.alibaba.flink.shuffle.transfer.ShuffleWriteClient;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * A transportation gate used to spill buffers from {@link ResultPartitionWriter} to remote shuffle
 * worker.
 */
public class RemoteShuffleOutputGate {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleOutputGate.class);

    /** A {@link ShuffleDescriptor} which describes shuffle meta and shuffle worker address. */
    private final RemoteShuffleDescriptor shuffleDesc;

    /** The total number of map partitions, which is equal to the upstream parallelism. */
    private final int numMapPartitions;

    /** Number of subpartitions of the corresponding {@link ResultPartitionWriter}. */
    protected final int numSubs;

    /** Used to transport data to a shuffle worker. */
    private final Map<Integer, ShuffleWriteClient> shuffleWriteClients = new HashMap<>();

    /** Used to consolidate buffers. */
    private final Map<Integer, BufferPacker> bufferPackers = new HashMap<>();

    /** {@link BufferPool} provider. */
    protected final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    /** Provides buffers to hold data to send online by Netty layer. */
    protected BufferPool bufferPool;

    /** Whether the data partition type is MapPartition. */
    private boolean isMapPartition;

    private final BlockingQueue<ReducePartitionWriteClient> pendingWriteClients =
            new LinkedBlockingQueue<>();

    /**
     * @param shuffleDesc Describes shuffle meta and shuffle worker address.
     * @param numSubs Number of subpartitions of the corresponding {@link ResultPartitionWriter}.
     * @param bufferPoolFactory {@link BufferPool} provider.
     * @param connectionManager Manages physical connections.
     */
    public RemoteShuffleOutputGate(
            RemoteShuffleDescriptor shuffleDesc,
            int numMapPartitions,
            int numSubs,
            int bufferSize,
            String dataPartitionFactoryName,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            ConnectionManager connectionManager) {

        this.shuffleDesc = shuffleDesc;
        this.numMapPartitions = numMapPartitions;
        this.numSubs = numSubs;
        this.bufferPoolFactory = bufferPoolFactory;
        initShuffleWriteClients(bufferSize, dataPartitionFactoryName, connectionManager);
    }

    boolean isMapPartition() {
        return isMapPartition;
    }

    public BlockingQueue<ReducePartitionWriteClient> getPendingWriteClients() {
        return pendingWriteClients;
    }

    ReducePartitionWriteClient takePendingWriteClient() throws InterruptedException {
        return pendingWriteClients.take();
    }

    boolean addPendingWriteClient(ReducePartitionWriteClient writeClient) {
        if (!pendingWriteClients.contains(writeClient)) {
            return pendingWriteClients.add(writeClient);
        }
        return false;
    }

    /** Initialize transportation gate. */
    public void setup() throws IOException, InterruptedException {
        bufferPool = CommonUtils.checkNotNull(bufferPoolFactory.get());
        CommonUtils.checkArgument(
                bufferPool.getNumberOfRequiredMemorySegments() >= 2,
                "Too few buffers for transfer, the minimum valid required size is 2.");

        // guarantee that we have at least one buffer
        BufferUtils.reserveNumRequiredBuffers(bufferPool, 1);
        for (ShuffleWriteClient writeClient : shuffleWriteClients.values()) {
            writeClient.open();
        }
    }

    /** Get transportation buffer pool. */
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public Map<Integer, ShuffleWriteClient> getShuffleWriteClients() {
        return shuffleWriteClients;
    }

    /** Writes a {@link Buffer} to a subpartition. */
    public void write(Buffer buffer, int subIdx) throws InterruptedException {
        if (isMapPartition) {
            checkState(bufferPackers.size() == 1, "Wrong buffer packer size.");
            for (BufferPacker bufferPacker : bufferPackers.values()) {
                bufferPacker.process(buffer, subIdx);
            }
        } else {
            BufferPacker bufferPacker = bufferPackers.get(subIdx);
            checkState(bufferPacker != null, "Buffer packer must not be null.");
            bufferPacker.writeWithoutCache(buffer, subIdx);
        }
    }

    /**
     * Indicates the start of a region. A region of buffers guarantees the records inside are
     * completed.
     *
     * @param isBroadcast Whether it's a broadcast region.
     */
    public void regionStart(boolean isBroadcast, SortBuffer sortBuffer, int bufferSize) {
        isBroadcast = getBroadcastState(isBroadcast);
        for (Map.Entry<Integer, ShuffleWriteClient> clientEntry : shuffleWriteClients.entrySet()) {
            int subPartitionIndex = clientEntry.getKey();
            ShuffleWriteClient shuffleWriteClient = clientEntry.getValue();
            long numSubpartitionBytes =
                    isMapPartition() ? 0 : sortBuffer.numSubpartitionBytes(subPartitionIndex);
            int requireCredit =
                    BufferUtils.calculateSubpartitionCredit(
                            numSubpartitionBytes,
                            0,
                            sortBuffer.numEvents(subPartitionIndex),
                            bufferSize);
            LOG.debug(
                    "{} write record {} bytes and {} events for {}, need {} credits. ",
                    this,
                    numSubpartitionBytes,
                    sortBuffer.numEvents(subPartitionIndex),
                    shuffleWriteClient,
                    requireCredit);
            shuffleWriteClient.regionStart(isBroadcast, numMapPartitions, requireCredit);
        }
    }

    public void regionStart(boolean isBroadcast, int targetSubpartition, int requireCredit) {
        isBroadcast = getBroadcastState(isBroadcast);
        shuffleWriteClients
                .get(targetSubpartition)
                .regionStart(isBroadcast, numMapPartitions, requireCredit);
    }

    // TODO, a ugly fix for broadcast mode. Fix this later.
    private boolean getBroadcastState(boolean isBroadcast) {
        return isMapPartition && isBroadcast;
    }

    /**
     * Indicates the finish of a region. A region is always bounded by a pair of region-start and
     * region-finish.
     */
    public void regionFinish() throws InterruptedException {
        for (BufferPacker bufferPacker : bufferPackers.values()) {
            bufferPacker.drain();
        }

        for (ShuffleWriteClient shuffleWriteClient : shuffleWriteClients.values()) {
            shuffleWriteClient.regionFinish();
        }
    }

    public void regionFinish(int targetSubpartition) throws InterruptedException {
        bufferPackers.get(targetSubpartition).drain();
        shuffleWriteClients.get(targetSubpartition).regionFinish();
    }

    public void checkAllWriteClientsRegionFinish() {
        for (ShuffleWriteClient shuffleWriteClient : shuffleWriteClients.values()) {
            checkState(
                    shuffleWriteClient.sentRegionFinish(),
                    "Has not sent region finish, "
                            + shuffleWriteClient.getChannelID()
                            + shuffleWriteClient.getMapID()
                            + shuffleWriteClient.getDataSetID());
        }
    }

    /** Indicates the writing/spilling is finished. */
    public void finish() throws InterruptedException {
        int numSentFinish = 0;
        for (Integer i : shuffleWriteClients.keySet()) {
            shuffleWriteClients.get(i).finish();
            numSentFinish++;
        }
        checkState(
                numSentFinish == shuffleWriteClients.size(),
                "Finish count doesn't match, " + numSentFinish + " " + shuffleWriteClients.size());
    }

    /** Close the transportation gate. */
    public void close() throws IOException {
        if (bufferPool != null) {
            bufferPool.lazyDestroy();
        }
        for (BufferPacker bufferPacker : bufferPackers.values()) {
            bufferPacker.close();
        }

        for (ShuffleWriteClient shuffleWriteClient : shuffleWriteClients.values()) {
            shuffleWriteClient.close();
        }
    }

    /** Returns shuffle descriptor. */
    public RemoteShuffleDescriptor getShuffleDesc() {
        return shuffleDesc;
    }

    private void initShuffleWriteClients(
            int bufferSize, String dataPartitionFactoryName, ConnectionManager connectionManager) {
        JobID jobID = shuffleDesc.getJobId();
        DataSetID dataSetID = shuffleDesc.getDataSetId();
        DataPartition.DataPartitionType partitionType =
                ((DefaultShuffleResource) shuffleDesc.getShuffleResource()).getDataPartitionType();
        isMapPartition = PartitionUtils.isMapPartition(partitionType);
        if (isMapPartition) {
            MapPartitionID mapID = (MapPartitionID) shuffleDesc.getDataPartitionID();
            ShuffleWorkerDescriptor workerDescriptor =
                    ((DefaultShuffleResource) shuffleDesc.getShuffleResource())
                            .getMapPartitionLocation();
            InetSocketAddress address =
                    new InetSocketAddress(
                            workerDescriptor.getWorkerAddress(), workerDescriptor.getDataPort());
            shuffleWriteClients.put(
                    0,
                    new ShuffleWriteClient(
                            address,
                            jobID,
                            dataSetID,
                            mapID,
                            numSubs,
                            bufferSize,
                            dataPartitionFactoryName,
                            connectionManager));
        } else {
            MapPartitionID mapID = (MapPartitionID) shuffleDesc.getDataPartitionID();
            ShuffleWorkerDescriptor[] workerDescriptors =
                    ((DefaultShuffleResource) shuffleDesc.getShuffleResource())
                            .getReducePartitionLocations();
            for (int i = 0; i < workerDescriptors.length; i++) {
                InetSocketAddress address =
                        new InetSocketAddress(
                                workerDescriptors[i].getWorkerAddress(),
                                workerDescriptors[i].getDataPort());
                if (shuffleWriteClients.containsKey(i)) {
                    continue;
                }
                shuffleWriteClients.put(
                        i,
                        new ReducePartitionWriteClient(
                                address,
                                jobID,
                                dataSetID,
                                mapID,
                                new ReducePartitionID(i),
                                numMapPartitions,
                                0,
                                0,
                                bufferSize,
                                dataPartitionFactoryName,
                                connectionManager,
                                pendingWriteClients::add));
            }
            checkState(
                    shuffleWriteClients.size() == workerDescriptors.length,
                    "Wrong write client count");
        }
        initBufferPackers();
    }

    private void initBufferPackers() {
        shuffleWriteClients.forEach(
                (subPartitionIndex, shuffleWriteClient) -> {
                    bufferPackers.put(
                            subPartitionIndex, new BufferPacker(shuffleWriteClient::write));
                });
    }
}

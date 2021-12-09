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

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReducePartitionWriteHandshakeRequest;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;

/**
 * Writer client used to retrieve buffers from a remote shuffle worker to Flink TM, which is used
 * when ReducePartition is enabled.
 */
public class ReducePartitionWriteClient extends ShuffleWriteClient {

    private static final Logger LOG = LoggerFactory.getLogger(ReducePartitionWriteClient.class);

    private final ReducePartitionID reduceID;

    /** The total number of map partitions, which is equal to the upstream parallelism. */
    private final int numMapPartitions;

    /** Index of the first logic Subpartition to be read (inclusive). */
    private final int startSubIdx;

    /** Index of the last logic Subpartition to be read (inclusive). */
    private final int endSubIdx;

    private final Consumer<ReducePartitionWriteClient> pendingWriteClientRegister;

    public ReducePartitionWriteClient(
            InetSocketAddress address,
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID mapID,
            ReducePartitionID reduceID,
            int numMapPartitions,
            int startSubIdx,
            int endSubIdx,
            int bufferSize,
            String dataPartitionFactoryName,
            ConnectionManager connectionManager,
            Consumer<ReducePartitionWriteClient> pendingWriteClientRegister) {

        super(
                address,
                jobID,
                dataSetID,
                mapID,
                Integer.MAX_VALUE,
                bufferSize,
                dataPartitionFactoryName,
                connectionManager);

        checkArgument(reduceID != null, "Must be not null.");
        checkArgument(numMapPartitions >= 0, "Must be positive value.");
        checkArgument(startSubIdx >= 0, "Must be positive value.");
        checkArgument(endSubIdx >= startSubIdx, "Must be equal or larger than startSubIdx.");
        this.numMapPartitions = numMapPartitions;
        this.reduceID = reduceID;
        this.startSubIdx = startSubIdx;
        this.endSubIdx = endSubIdx;
        this.pendingWriteClientRegister = pendingWriteClientRegister;
    }

    /** Initialize Netty connection and fire handshake. */
    @Override
    public void open() throws IOException, InterruptedException {
        LOG.debug("(remote: {}, channel: {}) Connect channel.", getAddress(), getChannelIDStr());
        setNettyChannel(getConnectionManager().getChannel(getChannelID(), getAddress()));
        setWriteClientHandler(getNettyChannel().pipeline().get(WriteClientHandler.class));

        if (getWriteClientHandler() == null) {
            throw new IOException(
                    "The network connection is already released for channelID: "
                            + getChannelIDStr());
        }
        getWriteClientHandler().register(this);

        ReducePartitionWriteHandshakeRequest msg =
                new ReducePartitionWriteHandshakeRequest(
                        currentProtocolVersion(),
                        getChannelID(),
                        getJobID(),
                        getDataSetID(),
                        getMapID(),
                        reduceID,
                        numMapPartitions,
                        startSubIdx,
                        endSubIdx,
                        getBufferSize(),
                        getDataPartitionFactoryName(),
                        emptyExtraMessage());
        LOG.debug("(remote: {}, channel: {}) Send {}.", getAddress(), getChannelIDStr(), msg);
        writeAndFlush(msg);
    }

    /** Writes a piece of data to a subpartition. */
    @Override
    public void write(ByteBuf byteBuf, int subIdx) throws InterruptedException {
        synchronized (lock) {
            try {
                healthCheck();
            } catch (Throwable t) {
                byteBuf.release();
                throw t;
            }

            int size = byteBuf.readableBytes();
            TransferMessage.WriteData writeData =
                    new TransferMessage.WriteData(
                            currentProtocolVersion(),
                            channelID,
                            byteBuf,
                            subIdx,
                            size,
                            false,
                            emptyExtraMessage());
            LOG.debug(
                    "(remote: {}, channel: {}) Send {}, credit {}.",
                    address,
                    channelIDStr,
                    writeData,
                    currentCredit);
            writeAndFlush(writeData);
            currentCredit--;
        }
    }

    /** Called by Netty thread. */
    @Override
    public void creditReceived(TransferMessage.WriteAddCredit addCredit) {
        LOG.debug(
                "Receive credit, credit region index: {}, current:{}, now {}, add {} {}, {}, same index? {}",
                addCredit.getRegionIdx(),
                currentRegionIdx,
                currentCredit,
                addCredit.getCredit(),
                getMapID(),
                reduceID,
                addCredit.getRegionIdx() == currentRegionIdx);

        synchronized (lock) {
            if (addCredit.getCredit() > 0 && addCredit.getRegionIdx() == currentRegionIdx) {
                if (addCredit.getRegionIdx() == currentRegionIdx) {
                    currentCredit += addCredit.getCredit();
                    if (isWaitingForCredit) {
                        lock.notifyAll();
                    }
                }
                pendingWriteClientRegister.accept(this);
            }
        }
    }

    public ReducePartitionID getReduceID() {
        return reduceID;
    }
}

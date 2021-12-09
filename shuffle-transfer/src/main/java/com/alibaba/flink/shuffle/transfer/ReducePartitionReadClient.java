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
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReducePartitionReadHandshakeRequest;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyOffset;

/**
 * Reader client used to retrieve buffers from a remote shuffle worker to Flink TM, which is used
 * when ReducePartition is enabled.
 */
public class ReducePartitionReadClient extends ShuffleReadClient {
    private static final Logger LOG = LoggerFactory.getLogger(ReducePartitionReadClient.class);

    /** {@link ReducePartitionID} of the reading. */
    private final ReducePartitionID reduceID;

    /** Number of subpartitions of the reading. */
    private final int numSubs;

    public ReducePartitionReadClient(
            InetSocketAddress address,
            DataSetID dataSetID,
            MapPartitionID mapID,
            ReducePartitionID reduceID,
            int numSubs,
            int bufferSize,
            TransferBufferPool bufferPool,
            ConnectionManager connectionManager,
            Consumer<ByteBuf> dataListener,
            Consumer<Throwable> failureListener) {

        super(
                address,
                dataSetID,
                mapID,
                0,
                0,
                bufferSize,
                bufferPool,
                connectionManager,
                dataListener,
                failureListener);

        checkArgument(reduceID != null, "Must be not null.");
        checkArgument(numSubs > 0, "Must be positive value.");
        this.reduceID = reduceID;
        this.numSubs = numSubs;
    }

    /** Fire handshake. */
    @Override
    public void open() throws IOException {
        setReadClientHandler(getNettyChannel().pipeline().get(ReadClientHandler.class));
        if (getReadClientHandler() == null) {
            throw new IOException(
                    "The network connection is already released for channelID: "
                            + getChannelIDStr());
        }
        getReadClientHandler().register(this);

        ReducePartitionReadHandshakeRequest handshake =
                new ReducePartitionReadHandshakeRequest(
                        currentProtocolVersion(),
                        getChannelID(),
                        getDataSetID(),
                        getMapID(),
                        reduceID,
                        numSubs,
                        0,
                        getBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage());
        LOG.debug("(remote: {}) Send {}.", getNettyChannel().remoteAddress(), handshake);
        getNettyChannel()
                .writeAndFlush(handshake)
                .addListener(
                        new ChannelFutureListenerImpl(
                                (ignored, throwable) -> exceptionCaught(throwable)));
    }
}

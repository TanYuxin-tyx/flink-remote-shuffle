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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.utils.ListenerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** {@link DataPartitionWriter} for {@link LocalFileReducePartition}. */
public class LocalFileReducePartitionWriter extends BaseReducePartitionWriter {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileReducePartitionWriter.class);

    protected int requiredCredit;

    protected int fulfilledCredit;

    private boolean isInputFinished;

    /** File writer used to write data to local file. */
    private final LocalReducePartitionFileWriter fileWriter;

    public LocalFileReducePartitionWriter(
            MapPartitionID mapPartitionID,
            BaseReducePartition dataPartition,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener,
            LocalReducePartitionFileWriter fileWriter) {
        super(mapPartitionID, dataPartition, dataRegionCreditListener, failureListener);
        this.fileWriter = fileWriter;
        this.isInputFinished = false;
    }

    @Override
    public void addBuffer(ReducePartitionID reducePartitionID, int dataRegionIndex, Buffer buffer) {
        LOG.debug(
                "Receive addBuffer, {}, id: {}, {}, regionID={}",
                dataPartition,
                dataPartition.getPartitionMeta().getDataPartitionID(),
                mapPartitionID,
                dataRegionIndex);
        super.addBuffer(reducePartitionID, dataRegionIndex, buffer);
    }

    @Override
    public void startRegion(
            int dataRegionIndex, int inputRequireCredit, boolean isBroadcastRegion) {
        super.startRegion(dataRegionIndex, inputRequireCredit, isBroadcastRegion);

        LOG.debug(
                "Receive startRegion, {}, id: {}, {}",
                dataPartition,
                dataPartition.getPartitionMeta().getDataPartitionID(),
                mapPartitionID);
        triggerWriting();
    }

    @Override
    public void finishRegion(int dataRegionIndex) {
        LOG.debug(
                "Receive finishRegion, {}, id: {}, {}, regionID={}",
                dataPartition,
                dataPartition.getPartitionMeta().getDataPartitionID(),
                mapPartitionID,
                dataRegionIndex);
        super.finishRegion(dataRegionIndex);
        needMoreCredits = false;
        triggerWriting();
    }

    @Override
    public void finishDataInput(DataCommitListener commitListener) {
        LOG.debug(
                "Receive finishDataInput, {}, id: {}, {}",
                dataPartition,
                dataPartition.getPartitionMeta().getDataPartitionID(),
                mapPartitionID);
        super.finishDataInput(commitListener);
        triggerWriting();
    }

    @Override
    protected void processRegionStartedMarker(BufferOrMarker.RegionStartedMarker marker)
            throws Exception {
        super.processRegionStartedMarker(marker);
        LOG.debug(
                "Process region start for "
                        + mapPartitionID
                        + " dataPartitionID="
                        + dataPartition.getPartitionMeta().getDataPartitionID()
                        + " regionID="
                        + currentDataRegionIndex);
        isRegionFinished = false;
        requiredCredit = marker.getRequireCredit();
        fulfilledCredit = 0;
        dataPartition.addPendingBufferWriter(this);
        fileWriter.startRegion(marker.isBroadcastRegion(), marker.getMapPartitionID());
        // Trigger writing again to dispatch buffers to the writer
        triggerWriting();
    }

    @Override
    protected void processDataBuffer(BufferOrMarker.DataBuffer buffer) throws Exception {
        if (!fileWriter.isOpened()) {
            fileWriter.open();
        }

        LOG.debug(
                "Process buffer "
                        + buffer.getBuffer().readableBytes()
                        + " for "
                        + mapPartitionID
                        + " dataPartitionID="
                        + dataPartition.getPartitionMeta().getDataPartitionID());
        // the file writer is responsible for releasing the target buffer
        fileWriter.writeBuffer(buffer);
    }

    @Override
    public Buffer pollBuffer() {
        synchronized (lock) {
            if (isReleased || isError) {
                throw new ShuffleException("Partition writer has been released or failed.");
            }

            Buffer buffer = availableCredits.poll();
            if (((BaseReducePartition) dataPartition).writingCounter == 1
                    && availableCredits.isEmpty()) {
                DataPartitionWritingTask writingTask =
                        CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
                writingTask.triggerWriting();
            }
            LOG.debug(
                    "Poll a buffer from data partition writer, {}, {}, {} available credits {}",
                    this.toString(),
                    mapPartitionID,
                    dataPartition.getPartitionMeta().getDataPartitionID(),
                    availableCredits.size());
            return buffer;
        }
    }

    @Override
    protected void processRegionFinishedMarker(BufferOrMarker.RegionFinishedMarker marker)
            throws Exception {
        LOG.debug(
                "Process region finish for "
                        + mapPartitionID
                        + " dataPartitionID="
                        + dataPartition.getPartitionMeta().getDataPartitionID()
                        + " regionID="
                        + marker.getDataRegionIndex());
        isRegionFinished = true;
        super.processRegionFinishedMarker(marker);

        fileWriter.finishRegion(marker.getMapPartitionID());
        dataPartition.decWritingCount();
    }

    @Override
    protected void processInputFinishedMarker(BufferOrMarker.InputFinishedMarker marker)
            throws Exception {
        LOG.debug(
                "Process input finish for "
                        + mapPartitionID
                        + " dataPartitionID="
                        + dataPartition.getPartitionMeta().getDataPartitionID());
        fileWriter.prepareFinishWriting(marker);

        // TODO, Why is the availableCredits not empty?
        releaseUnusedCredits();
        checkState(isRegionFinished, "The region should be stopped before the input is finished.");
        isInputFinished = true;
        if (areAllWritersFinished()) {
            fileWriter.closeWriting();
        }
        super.processInputFinishedMarker(marker);
    }

    @Override
    protected void addBufferOrMarker(BufferOrMarker bufferOrMarker) {
        boolean recycleBuffer;

        synchronized (lock) {
            if (!(recycleBuffer = isReleased)) {
                bufferOrMarkers.add(bufferOrMarker);
            }
        }

        if (recycleBuffer) {
            BufferOrMarker.releaseBuffer(bufferOrMarker);
            throw new ShuffleException("Partition writer has been released.");
        }
    }

    @Override
    public boolean assignCredits(BufferQueue credits, BufferRecycler recycler) {
        CommonUtils.checkArgument(credits != null, "Must be not null.");
        CommonUtils.checkArgument(recycler != null, "Must be not null.");

        needMoreCredits = !isCreditFulfilled() && !isRegionFinished;
        if (isReleased || isCreditFulfilled()) {
            return false;
        }

        //        if (credits.size() < MIN_CREDITS_TO_NOTIFY) {
        //            return needMoreCredits;
        //        }

        int numBuffers = 0;
        synchronized (lock) {
            if (isError) {
                return false;
            }

            while (credits.size() > 0) {
                ++numBuffers;
                availableCredits.add(new Buffer(credits.poll(), recycler, 0));
            }
        }
        LOG.debug(
                "Assigning buffers to data partition writer, {}, {}, {} region: {}, add new {} to available credits {},",
                this.toString(),
                mapPartitionID,
                dataPartition.getPartitionMeta().getDataPartitionID(),
                currentDataRegionIndex,
                numBuffers,
                availableCredits.size());

        fulfilledCredit += numBuffers;

        ListenerUtils.notifyAvailableCredits(
                numBuffers, currentDataRegionIndex, dataRegionCreditListener);
        needMoreCredits = !isCreditFulfilled() && !isRegionFinished;
        return needMoreCredits;
    }

    @Override
    public boolean isCreditFulfilled() {
        return fulfilledCredit >= requiredCredit;
    }

    @Override
    public int numPendingCredit() {
        return requiredCredit - fulfilledCredit;
    }

    @Override
    public int numFulfilledCredit() {
        return fulfilledCredit;
    }

    @Override
    public boolean isInputFinished() {
        return isInputFinished;
    }

    @Override
    public boolean isRegionFinished() {
        return isRegionFinished;
    }

    private boolean areAllWritersFinished() {
        for (Map.Entry<MapPartitionID, DataPartitionWriter> writerEntry :
                dataPartition.writers.entrySet()) {
            if (!writerEntry.getValue().isInputFinished()) {
                LOG.debug(
                        "The writer for {} {} have not finished the input process",
                        writerEntry.getKey(),
                        dataPartition.getPartitionMeta().getDataPartitionID());
                return false;
            }
        }
        LOG.debug("All writers have finished the input process");
        return true;
    }

    @Override
    public void release(Throwable throwable) throws Exception {
        Throwable error = null;

        try {
            super.release(throwable);
        } catch (Throwable exception) {
            error = exception;
            LOG.error("Failed to release base partition writer.", exception);
        }

        try {
            fileWriter.close();
        } catch (Throwable exception) {
            error = error == null ? exception : error;
            LOG.error("Failed to release file writer.", exception);
        }

        if (error != null) {
            ExceptionUtils.rethrowException(error);
        }
    }

    @Override
    protected Queue<BufferOrMarker> getPendingBufferOrMarkers() {
        synchronized (lock) {
            if (bufferOrMarkers.isEmpty()) {
                return null;
            }

            BufferOrMarker.Type type = bufferOrMarkers.getLast().getType();
            boolean shouldWriteData =
                    type == BufferOrMarker.Type.REGION_STARTED_MARKER
                            || type == BufferOrMarker.Type.REGION_FINISHED_MARKER
                            || type == BufferOrMarker.Type.INPUT_FINISHED_MARKER;
            LOG.debug("Should write data? {}, type={}", shouldWriteData, type);
            if (!shouldWriteData) {
                return null;
            }

            Queue<BufferOrMarker> pendingBufferOrMarkers = new ArrayDeque<>(bufferOrMarkers);
            bufferOrMarkers.clear();
            return pendingBufferOrMarkers;
        }
    }

    private void triggerWriting() {
        DataPartitionWritingTask writingTask =
                CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
        writingTask.triggerWriting();
    }
}

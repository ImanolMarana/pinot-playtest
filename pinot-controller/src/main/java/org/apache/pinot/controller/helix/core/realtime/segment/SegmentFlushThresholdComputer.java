/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.realtime.segment;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.TimeUtils;


class SegmentFlushThresholdComputer {
  public static final int MINIMUM_NUM_ROWS_THRESHOLD = 10_000;
  static final double CURRENT_SEGMENT_RATIO_WEIGHT = 0.1;
  static final double PREVIOUS_SEGMENT_RATIO_WEIGHT = 0.9;
  static final double ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT = 1.1;

  // num rows to segment size ratio of last committed segment for this table
  private double _latestSegmentRowsToSizeRatio;
  private final Clock _clock;

  SegmentFlushThresholdComputer() {
    this(Clock.systemUTC(), 0);
  }

  @VisibleForTesting
  SegmentFlushThresholdComputer(Clock clock, double latestSegmentRowsToSizeRatio) {
    _clock = clock;
    _latestSegmentRowsToSizeRatio = latestSegmentRowsToSizeRatio;
  }

  @VisibleForTesting
  SegmentFlushThresholdComputer(Clock clock) {
    this(clock, 0);
  }

  double getLatestSegmentRowsToSizeRatio() {
    return _latestSegmentRowsToSizeRatio;
  }

  public int computeThreshold(StreamConfig streamConfig, CommittingSegmentDescriptor committingSegmentDescriptor,
      @Nullable SegmentZKMetadata committingSegmentZKMetadata, String newSegmentName) {
    long desiredSegmentSizeBytes = getDesiredSegmentSizeBytes(streamConfig);
    long optimalSegmentSizeBytesMin = desiredSegmentSizeBytes / 2;
    double optimalSegmentSizeBytesMax = desiredSegmentSizeBytes * 1.5;

    if (committingSegmentZKMetadata == null) { // first segment of the partition
      return handleFirstSegment(newSegmentName, desiredSegmentSizeBytes, streamConfig);
    }

    final long committingSegmentSizeBytes = committingSegmentDescriptor.getSegmentSizeBytes();
    if (committingSegmentSizeBytes <= 0
        || SegmentCompletionProtocol.REASON_FORCE_COMMIT_MESSAGE_RECEIVED
            .equals(committingSegmentDescriptor.getStopReason())) {
      return handleForceCommitOrRepairSegment(newSegmentName, committingSegmentZKMetadata);
    }

    return computeThresholdBasedOnCommittingSegment(
        streamConfig, newSegmentName, committingSegmentSizeBytes, committingSegmentZKMetadata,
        optimalSegmentSizeBytesMin, optimalSegmentSizeBytesMax);
  }

  private int handleForceCommitOrRepairSegment(String newSegmentName,
      SegmentZKMetadata committingSegmentZKMetadata) {
    final int targetNumRows = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
    SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
        "Committing segment size is not available or segment is force-committed, setting thresholds from previous segment for {} as {}",
        newSegmentName, targetNumRows);
    return targetNumRows;
  }

  private int handleFirstSegment(String newSegmentName, long desiredSegmentSizeBytes,
      StreamConfig streamConfig) {
    if (_latestSegmentRowsToSizeRatio > 0) { // new partition group added case
      long targetSegmentNumRows = (long) (desiredSegmentSizeBytes * _latestSegmentRowsToSizeRatio);
      targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
      SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
          "Committing segment zk metadata is not available, using prev ratio {}, setting rows threshold for {} as {}",
          _latestSegmentRowsToSizeRatio, newSegmentName, targetSegmentNumRows);
      return (int) targetSegmentNumRows;
    } else {
      final int autotuneInitialRows = streamConfig.getFlushAutotuneInitialRows();
      SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
          "Committing segment zk metadata is not available, setting threshold for {} as {}",
          newSegmentName, autotuneInitialRows);
      return autotuneInitialRows;
    }
  }

  private long getDesiredSegmentSizeBytes(StreamConfig streamConfig) {
    long desiredSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    if (desiredSegmentSizeBytes <= 0) {
      desiredSegmentSizeBytes = StreamConfig.DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES;
    }
    return desiredSegmentSizeBytes;
  }

  private int computeThresholdBasedOnCommittingSegment(StreamConfig streamConfig,
      String newSegmentName, long committingSegmentSizeBytes,
      SegmentZKMetadata committingSegmentZKMetadata, long optimalSegmentSizeBytesMin,
      double optimalSegmentSizeBytesMax) {
    final long timeConsumed =
        _clock.millis() - committingSegmentZKMetadata.getCreationTime();
    final long numRowsConsumed = committingSegmentZKMetadata.getTotalDocs();
    final int numRowsThreshold = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
    SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
        "{}: Data from committing segment: Time {}  numRows {} threshold {} segmentSize(bytes) {}",
        newSegmentName, TimeUtils.convertMillisToPeriod(timeConsumed), numRowsConsumed,
        numRowsThreshold, committingSegmentSizeBytes);

    double currentRatio = (double) numRowsConsumed / committingSegmentSizeBytes;
    updateLatestSegmentRatio(currentRatio);

    if (numRowsConsumed < numRowsThreshold
        && committingSegmentSizeBytes < streamConfig.getFlushThresholdSegmentSizeBytes()) {
      return handleTimeThresholdReached(streamConfig, newSegmentName, numRowsConsumed,
          timeConsumed);
    }

    return computeThresholdBasedOnSegmentSize(newSegmentName, committingSegmentSizeBytes,
        optimalSegmentSizeBytesMin, optimalSegmentSizeBytesMax, currentRatio);
  }

  private int computeThresholdBasedOnSegmentSize(String newSegmentName,
      long committingSegmentSizeBytes, long optimalSegmentSizeBytesMin,
      double optimalSegmentSizeBytesMax, double currentRatio) {
    long targetSegmentNumRows;
    if (committingSegmentSizeBytes < optimalSegmentSizeBytesMin) {
      targetSegmentNumRows = numRowsConsumed + numRowsConsumed / 2;
    } else if (committingSegmentSizeBytes > optimalSegmentSizeBytesMax) {
      targetSegmentNumRows = numRowsConsumed / 2;
    } else {
      targetSegmentNumRows = getThresholdBasedOnRatio(currentRatio);
    }
    targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
    SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
        "Committing segment size {}, current ratio {}, setting threshold for {} as {}",
        committingSegmentSizeBytes, _latestSegmentRowsToSizeRatio, newSegmentName,
        targetSegmentNumRows);

    return (int) targetSegmentNumRows;
  }

  private int handleTimeThresholdReached(StreamConfig streamConfig, String newSegmentName,
      long numRowsConsumed, long timeConsumed) {
    final long timeThresholdMillis = streamConfig.getFlushThresholdTimeMillis();
    long currentNumRows = numRowsConsumed;
    StringBuilder logStringBuilder = new StringBuilder().append("Time threshold reached. ");
    if (timeThresholdMillis < timeConsumed) {
      // The administrator has reduced the time threshold. Adjust the
      // number of rows to match the average consumption rate on the partition.
      currentNumRows = timeThresholdMillis * numRowsConsumed / timeConsumed;
      logStringBuilder.append(" Detected lower time threshold, adjusting numRowsConsumed to ")
          .append(currentNumRows).append(". ");
    }
    long targetSegmentNumRows = (long) (currentNumRows * ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT);
    targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
    logStringBuilder.append("Setting segment size for {} as {}");
    SegmentSizeBasedFlushThresholdUpdater.LOGGER
        .info(logStringBuilder.toString(), newSegmentName, targetSegmentNumRows);
    return (int) targetSegmentNumRows;
  }

  private long getThresholdBasedOnRatio(double currentRatio) {
    long targetSegmentNumRows;
    if (_latestSegmentRowsToSizeRatio > 0) {
      targetSegmentNumRows =
          (long) (streamConfig.getFlushThresholdSegmentSizeBytes() * _latestSegmentRowsToSizeRatio);
    } else {
      targetSegmentNumRows =
          (long) (streamConfig.getFlushThresholdSegmentSizeBytes() * currentRatio);
    }
    return targetSegmentNumRows;
  }

  private void updateLatestSegmentRatio(double currentRatio) {
    if (_latestSegmentRowsToSizeRatio > 0) {
      _latestSegmentRowsToSizeRatio =
          CURRENT_SEGMENT_RATIO_WEIGHT * currentRatio
              + PREVIOUS_SEGMENT_RATIO_WEIGHT * _latestSegmentRowsToSizeRatio;
    } else {
      _latestSegmentRowsToSizeRatio = currentRatio;
    }
  }

  private long capNumRowsIfOverflow(long targetSegmentNumRows) {
    if (targetSegmentNumRows > Integer.MAX_VALUE) {
      // TODO Picking Integer.MAX_VALUE for number of rows will most certainly make the segment unloadable
      // so we need to pick a lower value here. But before that, we need to consider why the value may
      // go so high and prevent it. We will definitely reach a high segment size long before we get here...
      targetSegmentNumRows = Integer.MAX_VALUE;
    }
    return Math.max(targetSegmentNumRows, MINIMUM_NUM_ROWS_THRESHOLD);
  }
//Refactoring end

  private long capNumRowsIfOverflow(long targetSegmentNumRows) {
    if (targetSegmentNumRows > Integer.MAX_VALUE) {
      // TODO Picking Integer.MAX_VALUE for number of rows will most certainly make the segment unloadable
      // so we need to pick a lower value here. But before that, we need to consider why the value may
      // go so high and prevent it. We will definitely reach a high segment size long before we get here...
      targetSegmentNumRows = Integer.MAX_VALUE;
    }
    return Math.max(targetSegmentNumRows, MINIMUM_NUM_ROWS_THRESHOLD);
  }
}

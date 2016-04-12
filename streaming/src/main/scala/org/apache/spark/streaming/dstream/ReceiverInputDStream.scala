/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.{RateLimiter, Receiver}
import org.apache.spark.streaming.scheduler.{RateController, ReceivedBlockInfo, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.util.WriteAheadLogUtils

/**
 * Abstract class for defining any [[org.apache.spark.streaming.dstream.InputDStream]]
 * that has to start a receiver on worker nodes to receive external data.
 * Specific implementations of ReceiverInputDStream must
 * define [[getReceiver]] function that gets the receiver object of type
 * [[org.apache.spark.streaming.receiver.Receiver]] that will be sent
 * to the workers to receive data.
 * @param _ssc Streaming context that will execute this input stream
 * @tparam T Class type of the object of this stream
 */
abstract class ReceiverInputDStream[T: ClassTag](_ssc: StreamingContext)
  extends InputDStream[T](_ssc) {

  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new ReceiverRateController(id, RateEstimator.create(ssc.conf, ssc.graph.batchDuration)))
    } else {
      None
    }
  }

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a ReceiverInputDStream.
   */
  def getReceiver(): Receiver[T]

  // Nothing to start or stop as both taken care of by the ReceiverTracker.
  def start() {}

  def stop() {}

  /**
   * Generates RDDs with blocks received by the receiver of this stream. */
  override def compute(validTime: Time): Option[RDD[T]] = {
    val blockRDD = {

      if (validTime < graph.startTime) {
        // If this is called for any time before the start time of the context,
        // then this returns an empty RDD. This may happen when recovering from a
        // driver failure without any write ahead log to recover pre-failure data.
        new BlockRDD[T](ssc.sc, Array.empty)
      } else {
        // Otherwise, ask the tracker for all the blocks that have been allocated to this stream
        // for this batch
        val receiverTracker = ssc.scheduler.receiverTracker
        val blockInfos = receiverTracker.getBlocksOfBatch(validTime).getOrElse(id, Seq.empty)

        val rateLimitOption = if (!underRateControl) None else rateController.map {
            // Rate limits could have changed many times for a given batch, so we do a weighted sum
            _.asInstanceOf[ReceiverRateController].sumHistoryThenTrim(getTimeMillis())
          }

        // Register the input blocks information into InputInfoTracker
        val inputInfo = StreamInputInfo(id, blockInfos.flatMap(_.numRecords).sum, rateLimitOption)
        ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

        // Create the BlockRDD
        createBlockRDD(validTime, blockInfos)
      }
    }
    Some(blockRDD)
  }

  private[streaming] def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {

    if (blockInfos.nonEmpty) {
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray

      // Are WAL record handles present with all the blocks
      val areWALRecordHandlesPresent = blockInfos.forall { _.walRecordHandleOption.nonEmpty }

      if (areWALRecordHandlesPresent) {
        // If all the blocks have WAL record handle, then create a WALBackedBlockRDD
        val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
        val walRecordHandles = blockInfos.map { _.walRecordHandleOption.get }.toArray
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, blockIds, walRecordHandles, isBlockIdValid)
      } else {
        // Else, create a BlockRDD. However, if there are some blocks with WAL info but not
        // others then that is unexpected and log a warning accordingly.
        if (blockInfos.exists(_.walRecordHandleOption.nonEmpty)) {
          if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
            logError("Some blocks do not have Write Ahead Log information; " +
              "this is unexpected and data may not be recoverable after driver failures")
          } else {
            logWarning("Some blocks have Write Ahead Log information; this is unexpected")
          }
        }
        val validBlockIds = blockIds.filter { id =>
          ssc.sparkContext.env.blockManager.master.contains(id)
        }
        if (validBlockIds.length != blockIds.length) {
          logWarning("Some blocks could not be recovered as they were not found in memory. " +
            "To prevent such data loss, enable Write Ahead Log (see programming guide " +
            "for more details.")
        }
        new BlockRDD[T](ssc.sc, validBlockIds)
      }
    } else {
      // If no block is ready now, creating WriteAheadLogBackedBlockRDD or BlockRDD
      // according to the configuration
      if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, Array.empty, Array.empty, Array.empty)
      } else {
        new BlockRDD[T](ssc.sc, Array.empty)
      }
    }
  }

  /**
   * A RateController that sends the new rate to receivers, via the receiver tracker.
   */
  private[streaming] class ReceiverRateController(id: Int, estimator: RateEstimator)
      extends RateController(id, estimator) {

    private[streaming] case class RateLimitSnapshot(limit: Double, ts: Long)

    // a place to log all the rate limit changes for the current batch
    private[streaming] val rateLimitHistory: ArrayBuffer[RateLimitSnapshot] = ArrayBuffer()

    /**
     * Log the rateLimit change history, so that we can do a weighted sum later.
     *
     * @param rate the new rate
     * @param ts at which time the rate changed
     */
    private[streaming] def appendRateLimitToHistory(rate: Double, ts: Long) {
      rateLimitHistory.synchronized {
        rateLimitHistory += RateLimitSnapshot(rate, ts)
      }
    }

    /**
     * Calculate the rate limit weighted sum for the current batch.
     * Note this should be called for each batch once and only once.
     *
     * @param ts the ending timestamp of a batch
     * @return the rate limit weighted sum for the current batch
     */
    private[streaming] def sumHistoryThenTrim(ts: Long): Double = {
      var rateSum = 0D
      rateLimitHistory.synchronized {
        if (rateLimitHistory.isEmpty) {
          // return initialRate for the first batch
          val initialRate = RateLimiter.getMaxRateLimit(_ssc.sc.conf).toDouble
          rateLimitHistory += RateLimitSnapshot(initialRate, ts)
          rateSum = initialRate
        } else {
          /* for batches other than the first: */
          // (1) add a RateLimitSnapshot, which will be the ending of the current batch and
          //     the beginning of the next batch
          rateLimitHistory += RateLimitSnapshot(rateLimitHistory.last.limit, ts)

          // (2) then do a weighted sum
          val durationSum = rateLimitHistory.last.ts - rateLimitHistory.head.ts
          for (idx <- 0 until rateLimitHistory.length - 1) {
            val duration = rateLimitHistory(idx + 1).ts - rateLimitHistory(idx).ts
            rateSum += rateLimitHistory(idx).limit * duration / durationSum
          }

          // (3) then trim the history to the last one
          rateLimitHistory.trimStart(rateLimitHistory.length - 1)
        }
      }
      rateSum
    }

    override def publish(rate: Long): Unit = {
      ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
      // Also logs to the history for the current batch
      appendRateLimitToHistory(rate.toDouble, getTimeMillis())
    }
  }

  private def getTimeMillis() = ssc.scheduler.clock.getTimeMillis()
}

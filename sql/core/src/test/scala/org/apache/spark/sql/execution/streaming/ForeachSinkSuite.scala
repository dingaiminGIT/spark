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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.{TaskContext, TaskContextImpl, TaskKilledException}
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext

class ForeachSinkSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("foreach") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(2).write
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter())
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val expectedEventsForPartition0 = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 0),
        ForeachSinkSuite.Process(value = 1),
        ForeachSinkSuite.Process(value = 3),
        ForeachSinkSuite.Close(errorOrNull = null)
      )
      val expectedEventsForPartition1 = Seq(
        ForeachSinkSuite.Open(partition = 1, version = 0),
        ForeachSinkSuite.Process(value = 2),
        ForeachSinkSuite.Process(value = 4),
        ForeachSinkSuite.Close(errorOrNull = null)
      )

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 2)
      assert {
        allEvents === Seq(expectedEventsForPartition0, expectedEventsForPartition1) ||
          allEvents === Seq(expectedEventsForPartition1, expectedEventsForPartition0)
      }
      query.stop()
    }
  }

  test("foreach - error when in the process of ForeachWriter.process()") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).write
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def process(value: Int): Unit = {
            super.process(value)
            throw new RuntimeException("error")
          }
        })
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachSinkSuite.Open(partition = 0, version = 0))
      assert(allEvents(0)(1) ===  ForeachSinkSuite.Process(value = 1))
      val errorEvent = allEvents(0)(2).asInstanceOf[ForeachSinkSuite.Close]
      assert(errorEvent.errorOrNull.isInstanceOf[RuntimeException])
      assert(errorEvent.errorOrNull.getMessage === "error")
      query.stop()
    }
  }

  test("foreach - task gets killed when in the process of ForeachWriter.open()") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).write
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def open(partitionId: Long, version: Long): Boolean = {
            events += ForeachSinkSuite.Open(partition = partitionId, version = version)
            // we've opened and occupied resources; before we can return true, our task gets killed
            TaskUtil.mockCurrentTaskBeingKilled()
            true // this line is not reachable
          }
        })
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachSinkSuite.Open(partition = 0, version = 0))
      // below checks that ForeachWriter.close() gets called with TaskKilledException
      val errorEvent = allEvents(0)(1).asInstanceOf[ForeachSinkSuite.Close]
      assert(errorEvent.errorOrNull.isInstanceOf[TaskKilledException])
      assert(errorEvent.errorOrNull.getMessage === null)

      query.stop()
    }
  }

  test("foreach - task gets killed when in the process of ForeachWriter.process()") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).write
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def process(value: Int): Unit = {
            super.process(value)
            TaskUtil.mockCurrentTaskBeingKilled()
          }
        })
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachSinkSuite.Open(partition = 0, version = 0))
      assert(allEvents(0)(1) ===  ForeachSinkSuite.Process(value = 1))
      // below checks that ForeachWriter.close() gets called with TaskKilledException
      val errorEvent = allEvents(0)(2).asInstanceOf[ForeachSinkSuite.Close]
      assert(errorEvent.errorOrNull.isInstanceOf[TaskKilledException])
      assert(errorEvent.errorOrNull.getMessage === null)

      query.stop()
    }
  }

  test("foreach - task gets killed when in the process of ForeachWriter.close()") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).write
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          private var firstTime = true
          // we would enter this close() method twice:
          // (1) the first time we enter this method, our task would get killed before we can
          // actually release the resources we've occupied
          // (2) the second time we enter this, we would complete the actual close() process, as
          // our task would not be killed for a second time
          override def close(errorOrNull: Throwable): Unit = {
            if (firstTime) {
              firstTime = false
              TaskUtil.mockCurrentTaskBeingKilled()
            }
            super.close(errorOrNull)
          }
        })
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachSinkSuite.Open(partition = 0, version = 0))
      for (num <- 1 to 4) {
        assert(allEvents(0)(num) === ForeachSinkSuite.Process(value = num))
      }
      // below checks that ForeachWriter.close() gets called with TaskKilledException
      val errorEvent = allEvents(0)(5).asInstanceOf[ForeachSinkSuite.Close]
      assert(errorEvent.errorOrNull.isInstanceOf[TaskKilledException])
      assert(errorEvent.errorOrNull.getMessage === null)

      query.stop()
    }
  }
}

private object TaskUtil {
  /**
   * To mock the current task being killed, we first mark current TaskContextImpl's
   * `interrupted` flag to true, then throw an InterruptedException in current thread.
   *
   * This is roughly what a task would behave when it gets killed by the TaskRunner (please refer
   * to [[org.apache.spark.scheduler.Task.kill()]] for details).
   */
  def mockCurrentTaskBeingKilled() = {
    TaskContext.get().asInstanceOf[TaskContextImpl].markInterrupted()
    throw new InterruptedException
  }
}

/** A global object to collect events in the executor */
object ForeachSinkSuite {

  trait Event

  case class Open(partition: Long, version: Long) extends Event

  case class Process[T](value: T) extends Event

  case class Close(errorOrNull: Throwable) extends Event

  private val _allEvents = new ConcurrentLinkedQueue[Seq[Event]]()

  def addEvents(events: Seq[Event]): Unit = {
    _allEvents.add(events)
  }

  def allEvents(): Seq[Seq[Event]] = {
    _allEvents.toArray(new Array[Seq[Event]](_allEvents.size()))
  }

  def clear(): Unit = {
    _allEvents.clear()
  }
}

/** A [[ForeachWriter]] that writes collected events to ForeachSinkSuite */
class TestForeachWriter extends ForeachWriter[Int] {
  ForeachSinkSuite.clear()

  protected val events = mutable.ArrayBuffer[ForeachSinkSuite.Event]()

  override def open(partitionId: Long, version: Long): Boolean = {
    events += ForeachSinkSuite.Open(partition = partitionId, version = version)
    true
  }

  override def process(value: Int): Unit = {
    events += ForeachSinkSuite.Process(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    events += ForeachSinkSuite.Close(errorOrNull)
    ForeachSinkSuite.addEvents(events)
  }
}

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

package org.apache.spark.executor

import java.util.concurrent.CountDownLatch

import org.apache.spark.TaskState.TaskState
import org.apache.spark._
import org.apache.spark.memory.MemoryManager
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{FakeTask, Task}
import org.apache.spark.serializer.JavaSerializer
import org.mockito.Matchers._
import org.mockito.Mockito.{mock, spy, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.mutable.HashMap

class ExecutorSuite extends SparkFunSuite with LocalSparkContext {


  test("mutating values2") {

    // We mock some objects so that we can later make Executor.launchTask() happy
    val conf = new SparkConf
    val serializer = new JavaSerializer(conf)
    val mockEnv = mock(classOf[SparkEnv])
    val mockRpcEnv = mock(classOf[RpcEnv])
    val mockMemoryManager = mock(classOf[MemoryManager])
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.rpcEnv).thenReturn(mockRpcEnv)
    when(mockEnv.memoryManager).thenReturn(mockMemoryManager)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    val serializedTask =
      Task.serializeWithDependencies(
        new FakeTask(0),
        HashMap[String, Long](),
        HashMap[String, Long](),
        serializer.newInstance())

    /**
     * +------------------------+--------------------+
     * | main test thread           thread pool       |
     * ------------------------------------------------
     * |executor.launchTask() ----->
     * |                              TaskRunner.run()
     * |                             execBackend.statusUpdate#L240
     * |executor.killAllTasks(true)
     * |                                  ...
     * |                            task = ser.deserialize...#L253
     * |                                  ...
     * |                            execBackend.statusUpdate#L365, not 401  <- assertThis
     * |
     *
     *
     */
    val mockExecutorBackend = mock(classOf[ExecutorBackend])
    when(mockExecutorBackend.statusUpdate(any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
        var firstTime = true

        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          if (firstTime) {
            TestHelper.latch1.countDown()
            TestHelper.latch2.await()
            firstTime = false
          }
          else {
            val taskState = invocationOnMock.getArguments()(1).asInstanceOf[TaskState]
            TestHelper.taskState = taskState
            TestHelper.latch3.countDown()
          }
        }
      })
    val executor = spy(new Executor("", "", mockEnv, Nil, isLocal = true))

    executor.launchTask(mockExecutorBackend, 0, 0, "", serializedTask)

    TestHelper.latch1.await()
    executor.killAllTasks(true)
    TestHelper.latch2.countDown()
    TestHelper.latch3.await()
    assert(TestHelper.taskState == TaskState.FAILED)
  }
}

private object TestHelper {

  val latch1 = new CountDownLatch(1)
  val latch2 = new CountDownLatch(1)
  val latch3 = new CountDownLatch(1)

  var taskState: TaskState = _
}

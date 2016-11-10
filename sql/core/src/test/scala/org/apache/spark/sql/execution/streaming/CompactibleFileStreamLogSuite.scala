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

import java.io._
import java.nio.charset.StandardCharsets._

import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.streaming.FakeFileSystem._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSQLContext

class CompactibleFileStreamLogSuite extends SparkFunSuite with SharedSQLContext {

  /** To avoid caching of FS objects */
  override protected val sparkConf =
    new SparkConf().set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true")

  import CompactibleFileStreamLog._

  testWithUninterruptibleThread("knownCompactionBatches and zeroBatch") {
    withTempDir { dir =>
      def newFakeCompactibleFileStreamLog(compactInterval: Int) =
        new FakeCompactibleFileStreamLog(
          _fileCleanupDelayMs = Long.MaxValue,
          _compactInterval = compactInterval,
          spark,
          dir.getCanonicalPath)

      var compactibleLog = newFakeCompactibleFileStreamLog(2)
      assert(compactibleLog.knownCompactionBatches === Array())
      assert(compactibleLog.zeroBatch === 0)
      compactibleLog.add(0, Array("entry_0"))
      compactibleLog.add(1, Array("entry_1")) // should compact

      compactibleLog = newFakeCompactibleFileStreamLog(2)
      assert(compactibleLog.knownCompactionBatches === Array(1))
      assert(compactibleLog.zeroBatch === 2)
      compactibleLog.add(2, Array("entry_2"))
      compactibleLog.add(3, Array("entry_3")) // should compact

      compactibleLog = newFakeCompactibleFileStreamLog(3)
      assert(compactibleLog.knownCompactionBatches === Array(1, 3))
      assert(compactibleLog.zeroBatch === 4)
      compactibleLog.add(4, Array("entry_4"))
      compactibleLog.add(5, Array("entry_5"))
      compactibleLog.add(6, Array("entry_6")) // should compact

      compactibleLog = newFakeCompactibleFileStreamLog(5)
      assert(compactibleLog.knownCompactionBatches === Array(1, 3, 6))
      assert(compactibleLog.zeroBatch === 7)
      compactibleLog.add(7, Array("entry_7"))
      compactibleLog.add(8, Array("entry_8"))
      compactibleLog.add(9, Array("entry_9"))
      compactibleLog.add(10, Array("entry_10"))

      compactibleLog = newFakeCompactibleFileStreamLog(2)
      assert(compactibleLog.knownCompactionBatches === Array(1, 3, 6))
      assert(compactibleLog.zeroBatch === 11)
    }
  }

  private val emptyKnownCompactionBatches = Array[Long]()
  private val knownCompactionBatches = Array[Long](
    1, 3, // produced with interval = 2
    6     // produced with interval = 3
  )

  /** -- testing of `object CompactibleFileStreamLog` begins -- */

  test("getBatchIdFromFileName") {
    assert(1234L === getBatchIdFromFileName("1234"))
    assert(1234L === getBatchIdFromFileName("1234.compact"))
    intercept[NumberFormatException] {
      getBatchIdFromFileName("1234a")
    }
  }

  test("isCompactionBatchFromFileName") {
    assert(false === isCompactionBatchFromFileName("1234"))
    assert(true === isCompactionBatchFromFileName("1234.compact"))
  }

  test("isCompactionBatch") {
    // test empty knownCompactionBatches cases
    assert(false === isCompactionBatch(emptyKnownCompactionBatches, 0, 0, compactInterval = 3))
    assert(false === isCompactionBatch(emptyKnownCompactionBatches, 1, 0, compactInterval = 3))
    assert(true === isCompactionBatch(emptyKnownCompactionBatches, 2, 0, compactInterval = 3))
    assert(false === isCompactionBatch(emptyKnownCompactionBatches, 3, 0, compactInterval = 3))
    assert(false === isCompactionBatch(emptyKnownCompactionBatches, 4, 0, compactInterval = 3))
    assert(true === isCompactionBatch(emptyKnownCompactionBatches, 5, 0, compactInterval = 3))

    // test non-empty knownCompactionBatches cases
    assert(false === isCompactionBatch(knownCompactionBatches, 0, 7, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 1, 7, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 2, 7, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 3, 7, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 4, 7, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 5, 7, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 6, 7, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 7, 7, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 8, 7, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 9, 7, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 10, 7, compactInterval = 3))

    assert(false === isCompactionBatch(knownCompactionBatches, 0, 20, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 1, 20, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 2, 20, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 3, 20, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 4, 20, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 5, 20, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 6, 20, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 7, 20, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 8, 20, compactInterval = 3))
    // notice the following one, it should be false !!!
    assert(false === isCompactionBatch(knownCompactionBatches, 9, 20, compactInterval = 3))
    for (batchId <- 10 until 20) {
      assert(false === isCompactionBatch(knownCompactionBatches, batchId, 20, compactInterval = 3))
    }
    assert(false === isCompactionBatch(knownCompactionBatches, 20, 20, compactInterval = 3))
    assert(false === isCompactionBatch(knownCompactionBatches, 21, 20, compactInterval = 3))
    assert(true === isCompactionBatch(knownCompactionBatches, 22, 20, compactInterval = 3))
  }

  test("nextCompactionBatchId") {
    // test empty knownCompactionBatches cases
    assert(2 === nextCompactionBatchId(emptyKnownCompactionBatches, 0, 0, compactInterval = 3))
    assert(2 === nextCompactionBatchId(emptyKnownCompactionBatches, 1, 0, compactInterval = 3))
    assert(5 === nextCompactionBatchId(emptyKnownCompactionBatches, 2, 0, compactInterval = 3))
    assert(5 === nextCompactionBatchId(emptyKnownCompactionBatches, 3, 0, compactInterval = 3))
    assert(5 === nextCompactionBatchId(emptyKnownCompactionBatches, 4, 0, compactInterval = 3))
    assert(8 === nextCompactionBatchId(emptyKnownCompactionBatches, 5, 0, compactInterval = 3))

    // test non-empty knownCompactionBatches cases
    assert(8 === nextCompactionBatchId(knownCompactionBatches, 7, 7, compactInterval = 2))
    assert(10 === nextCompactionBatchId(knownCompactionBatches, 8, 7, compactInterval = 2))
    assert(10 === nextCompactionBatchId(knownCompactionBatches, 9, 7, compactInterval = 2))
    assert(12 === nextCompactionBatchId(knownCompactionBatches, 10, 7, compactInterval = 2))
    assert(12 === nextCompactionBatchId(knownCompactionBatches, 11, 7, compactInterval = 2))
    assert(9 === nextCompactionBatchId(knownCompactionBatches, 7, 7, compactInterval = 3))
    assert(9 === nextCompactionBatchId(knownCompactionBatches, 8, 7, compactInterval = 3))
    assert(12 === nextCompactionBatchId(knownCompactionBatches, 9, 7, compactInterval = 3))
    assert(12 === nextCompactionBatchId(knownCompactionBatches, 10, 7, compactInterval = 3))
    assert(12 === nextCompactionBatchId(knownCompactionBatches, 11, 7, compactInterval = 3))

    assert(11 === nextCompactionBatchId(knownCompactionBatches, 10, 10, compactInterval = 2))
    assert(13 === nextCompactionBatchId(knownCompactionBatches, 11, 10, compactInterval = 2))
    assert(13 === nextCompactionBatchId(knownCompactionBatches, 12, 10, compactInterval = 2))
    assert(15 === nextCompactionBatchId(knownCompactionBatches, 13, 10, compactInterval = 2))
    assert(15 === nextCompactionBatchId(knownCompactionBatches, 14, 10, compactInterval = 2))
    assert(12 === nextCompactionBatchId(knownCompactionBatches, 10, 10, compactInterval = 3))
    assert(12 === nextCompactionBatchId(knownCompactionBatches, 11, 10, compactInterval = 3))
    assert(15 === nextCompactionBatchId(knownCompactionBatches, 12, 10, compactInterval = 3))
    assert(15 === nextCompactionBatchId(knownCompactionBatches, 13, 10, compactInterval = 3))
    assert(15 === nextCompactionBatchId(knownCompactionBatches, 14, 10, compactInterval = 3))
  }

  test("getValidBatchesBeforeCompactionBatch") {
    // test empty knownCompactionBatches cases
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(emptyKnownCompactionBatches, 0, 0, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(emptyKnownCompactionBatches, 1, 0, compactInterval = 3)
    }
    assert(Seq(0, 1) ===
      getValidBatchesBeforeCompactionBatch(emptyKnownCompactionBatches, 2, 0, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(emptyKnownCompactionBatches, 3, 0, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(emptyKnownCompactionBatches, 4, 0, compactInterval = 3)
    }
    assert(Seq(2, 3, 4) ===
      getValidBatchesBeforeCompactionBatch(emptyKnownCompactionBatches, 5, 0, compactInterval = 3))

    // test non-empty knownCompactionBatches cases
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 7, 7, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 8, 7, compactInterval = 3)
    }
    assert(Seq(6, 7, 8) ===
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 9, 7, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 10, 7, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 11, 7, compactInterval = 3)
    }
    assert(Seq(9, 10, 11) ===
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 12, 7, compactInterval = 3))

    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 20, 20, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 21, 20, compactInterval = 3)
    }
    assert((6 to 21) ===
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 22, 20, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 23, 20, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 24, 20, compactInterval = 3)
    }
    assert(Seq(22, 23, 24) ===
      getValidBatchesBeforeCompactionBatch(knownCompactionBatches, 25, 20, compactInterval = 3))
  }

  test("getAllValidBatches") {
    // test empty knownCompactionBatches cases
    assert(
      Seq(0) === getAllValidBatches(emptyKnownCompactionBatches, 0, 0, compactInterval = 3))
    assert(
      Seq(0, 1) === getAllValidBatches(emptyKnownCompactionBatches, 1, 0, compactInterval = 3))
    assert(
      Seq(2) === getAllValidBatches(emptyKnownCompactionBatches, 2, 0, compactInterval = 3))
    assert(
      Seq(2, 3) === getAllValidBatches(emptyKnownCompactionBatches, 3, 0, compactInterval = 3))
    assert(
      Seq(2, 3, 4) === getAllValidBatches(emptyKnownCompactionBatches, 4, 0, compactInterval = 3))
    assert(
      Seq(5) === getAllValidBatches(emptyKnownCompactionBatches, 5, 0, compactInterval = 3))
    assert(
      Seq(5, 6) === getAllValidBatches(emptyKnownCompactionBatches, 6, 0, compactInterval = 3))
    assert(
      Seq(5, 6, 7) === getAllValidBatches(emptyKnownCompactionBatches, 7, 0, compactInterval = 3))
    assert(
      Seq(8) === getAllValidBatches(emptyKnownCompactionBatches, 8, 0, compactInterval = 3))

    // test non-empty knownCompactionBatches cases
    assert(
      Seq(0) === getAllValidBatches(knownCompactionBatches, 0, 7, compactInterval = 3))
    assert(
      Seq(1) === getAllValidBatches(knownCompactionBatches, 1, 7, compactInterval = 3))
    assert(
      Seq(1, 2) === getAllValidBatches(knownCompactionBatches, 2, 7, compactInterval = 3))
    assert(
      Seq(3) === getAllValidBatches(knownCompactionBatches, 3, 7, compactInterval = 3))
    assert(
      Seq(3, 4) === getAllValidBatches(knownCompactionBatches, 4, 7, compactInterval = 3))
    assert(
      Seq(3, 4, 5) === getAllValidBatches(knownCompactionBatches, 5, 7, compactInterval = 3))
    assert(
      Seq(6) === getAllValidBatches(knownCompactionBatches, 6, 7, compactInterval = 3))
    assert(
      Seq(6, 7) === getAllValidBatches(knownCompactionBatches, 7, 7, compactInterval = 3))
    assert(
      Seq(6, 7, 8) === getAllValidBatches(knownCompactionBatches, 8, 7, compactInterval = 3))
    assert(
      Seq(9) === getAllValidBatches(knownCompactionBatches, 9, 7, compactInterval = 3))
    assert(
      Seq(9, 10) === getAllValidBatches(knownCompactionBatches, 10, 7, compactInterval = 3))
    assert(
      Seq(9, 10, 11) === getAllValidBatches(knownCompactionBatches, 11, 7, compactInterval = 3))
    assert(
      Seq(12) === getAllValidBatches(knownCompactionBatches, 12, 7, compactInterval = 3))
    assert(
      Seq(12, 13) === getAllValidBatches(knownCompactionBatches, 13, 7, compactInterval = 3))
    assert(
      Seq(12, 13, 14) === getAllValidBatches(knownCompactionBatches, 14, 7, compactInterval = 3))
    assert(
      Seq(15) === getAllValidBatches(knownCompactionBatches, 15, 7, compactInterval = 3))

    assert(
      (6 to 20) === getAllValidBatches(knownCompactionBatches, 20, 20, compactInterval = 3))
    assert(
      (6 to 21) === getAllValidBatches(knownCompactionBatches, 21, 20, compactInterval = 3))
    assert(
      Seq(22) === getAllValidBatches(knownCompactionBatches, 22, 20, compactInterval = 3))
    assert(
      Seq(22, 23) === getAllValidBatches(knownCompactionBatches, 23, 20, compactInterval = 3))
    assert(
      Seq(22, 23, 24) === getAllValidBatches(knownCompactionBatches, 24, 20, compactInterval = 3))
    assert(
      Seq(25) === getAllValidBatches(knownCompactionBatches, 25, 20, compactInterval = 3))
    assert(
      Seq(25, 26) === getAllValidBatches(knownCompactionBatches, 26, 20, compactInterval = 3))
    assert(
      Seq(25, 26, 27) === getAllValidBatches(knownCompactionBatches, 27, 20, compactInterval = 3))
    assert(
      Seq(28) === getAllValidBatches(knownCompactionBatches, 28, 20, compactInterval = 3))
  }

  /** -- testing of `object CompactibleFileStreamLog` ends -- */

  test("batchIdToPath") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      compactInterval = 3,
      existingFiles = Seq[String](),
      compactibleLog => {
        assert("0" === compactibleLog.batchIdToPath(0).getName)
        assert("1" === compactibleLog.batchIdToPath(1).getName)
        assert("2.compact" === compactibleLog.batchIdToPath(2).getName)
        assert("3" === compactibleLog.batchIdToPath(3).getName)
        assert("4" === compactibleLog.batchIdToPath(4).getName)
        assert("5.compact" === compactibleLog.batchIdToPath(5).getName)
      })

    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      compactInterval = 3,
      existingFiles = Seq[String](
        "0", "1.compact",
        "2", "3.compact",
        "4", "5"
      ),
      compactibleLog => {
        assert("0" === compactibleLog.batchIdToPath(0).getName)
        assert("1.compact" === compactibleLog.batchIdToPath(1).getName)
        assert("2" === compactibleLog.batchIdToPath(2).getName)
        assert("3.compact" === compactibleLog.batchIdToPath(3).getName)
        assert("4" === compactibleLog.batchIdToPath(4).getName)
        assert("5" === compactibleLog.batchIdToPath(5).getName)
        assert("6" === compactibleLog.batchIdToPath(6).getName)
        assert("7" === compactibleLog.batchIdToPath(7).getName)
        assert("8.compact" === compactibleLog.batchIdToPath(8).getName)
        assert("9" === compactibleLog.batchIdToPath(9).getName)
        assert("10" === compactibleLog.batchIdToPath(10).getName)
        assert("11.compact" === compactibleLog.batchIdToPath(11).getName)
      })
  }

    test("serialize") {
      withFakeCompactibleFileStreamLog(
        fileCleanupDelayMs = Long.MaxValue,
        compactInterval = 3,
        existingFiles = Seq[String](),
        compactibleLog => {
          val logs = Array("entry_1", "entry_2", "entry_3")
          val expected = s"""test_version
              |entry_1
              |entry_2
              |entry_3""".stripMargin
          val baos = new ByteArrayOutputStream()
          compactibleLog.serialize(logs, baos)
          assert(expected === baos.toString(UTF_8.name()))

          baos.reset()
          compactibleLog.serialize(Array(), baos)
          assert("test_version" === baos.toString(UTF_8.name()))
        })
    }

    test("deserialize") {
      withFakeCompactibleFileStreamLog(
        fileCleanupDelayMs = Long.MaxValue,
        compactInterval = 3,
        existingFiles = Seq[String](),
        compactibleLog => {
          val logs = s"""test_version
              |entry_1
              |entry_2
              |entry_3""".stripMargin
          val expected = Array("entry_1", "entry_2", "entry_3")
          assert(expected ===
            compactibleLog.deserialize(new ByteArrayInputStream(logs.getBytes(UTF_8))))

          assert(Nil ===
            compactibleLog.deserialize(new ByteArrayInputStream("test_version".getBytes(UTF_8))))
        })
    }

    testWithUninterruptibleThread("compact") {
      withFakeCompactibleFileStreamLog(
        fileCleanupDelayMs = Long.MaxValue,
        compactInterval = 3,
        existingFiles = Seq[String](),
        compactibleLog => {
          for (batchId <- 0 to 10) {
            compactibleLog.add(batchId, Array("some_path_" + batchId))
            val expectedFiles = (0 to batchId).map { id => "some_path_" + id }
            assert(compactibleLog.allFiles() === expectedFiles)
            if (isCompactionBatch(emptyKnownCompactionBatches, batchId, 0, 3)) {
              // Since batchId is a compaction batch, the batch log file should contain all logs
              assert(compactibleLog.get(batchId).getOrElse(Nil) === expectedFiles)
            }
          }
        })
    }

    testWithUninterruptibleThread("delete expired file") {
      // Set `fileCleanupDelayMs` to 0 so that we can detect the deleting behaviour deterministically
      withFakeCompactibleFileStreamLog(
        fileCleanupDelayMs = 0,
        compactInterval = 3,
        existingFiles = Seq[String](),
        compactibleLog => {
          val fs = compactibleLog.metadataPath.getFileSystem(spark.sessionState.newHadoopConf())

          def listBatchFiles(): Set[String] = {
            fs.listStatus(compactibleLog.metadataPath).map(_.getPath.getName).filter { fileName =>
              try {
                getBatchIdFromFileName(fileName)
                true
              } catch {
                case _: NumberFormatException => false
              }
            }.toSet
          }

          compactibleLog.add(0, Array("some_path_0"))
          assert(Set("0") === listBatchFiles())
          compactibleLog.add(1, Array("some_path_1"))
          assert(Set("0", "1") === listBatchFiles())
          compactibleLog.add(2, Array("some_path_2"))
          assert(Set("2.compact") === listBatchFiles())
          compactibleLog.add(3, Array("some_path_3"))
          assert(Set("2.compact", "3") === listBatchFiles())
          compactibleLog.add(4, Array("some_path_4"))
          assert(Set("2.compact", "3", "4") === listBatchFiles())
          compactibleLog.add(5, Array("some_path_5"))
          assert(Set("5.compact") === listBatchFiles())
        })
    }

  private def withFakeCompactibleFileStreamLog(
    fileCleanupDelayMs: Long,
    compactInterval: Int,
    existingFiles: Seq[String],
    f: FakeCompactibleFileStreamLog => Unit
  ): Unit = {
    withTempDir { dir =>
      for (existingFile <- existingFiles) {
        new File(dir, existingFile).createNewFile()
      }
      val compactibleLog = new FakeCompactibleFileStreamLog(
        fileCleanupDelayMs,
        compactInterval,
        spark,
        dir.getCanonicalPath)
      f(compactibleLog)
    }
  }
}

class FakeCompactibleFileStreamLog(
    _fileCleanupDelayMs: Long,
    _compactInterval: Int,
    sparkSession: SparkSession,
    path: String)
  extends CompactibleFileStreamLog[String]("test_version", sparkSession, path) {

  override protected def fileCleanupDelayMs: Long = _fileCleanupDelayMs

  override protected def isDeletingExpiredLog: Boolean = true

  override protected def compactInterval: Int = _compactInterval

  override protected def serializeData(t: String): String = t

  override protected def deserializeData(encodedString: String): String = encodedString

  override def compactLogs(logs: Seq[String]): Seq[String] = logs
}

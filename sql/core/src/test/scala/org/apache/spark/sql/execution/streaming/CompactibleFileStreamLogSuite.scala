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
import java.net.URI
import java.nio.charset.StandardCharsets._
import java.util.ConcurrentModificationException

import scala.language.implicitConversions
import scala.util.Random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.AsyncAssertions._
import org.scalatest.time.SpanSugar._
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.streaming.FakeFileSystem._
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog.{FileContextManager, FileManager, FileSystemManager}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.UninterruptibleThread

import scala.reflect.ClassTag

class CompactibleFileStreamLogSuite extends SparkFunSuite with SharedSQLContext {

  /** To avoid caching of FS objects */
  override protected val sparkConf =
    new SparkConf().set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true")

  import CompactibleFileStreamLog._

  private implicit def toOption[A](a: A): Option[A] = Option(a)

  test("getBatchIdFromFileName") {
    assert(1234L === getBatchIdFromFileName("1234"))
    assert(1234L === getBatchIdFromFileName("1234.compact"))
    intercept[NumberFormatException] {
      getBatchIdFromFileName("1234a")
    }
  }

  test("isCompactionBatch") {
    assert(false === isCompactionBatch(0, compactInterval = 3))
    assert(false === isCompactionBatch(1, compactInterval = 3))
    assert(true === isCompactionBatch(2, compactInterval = 3))
    assert(false === isCompactionBatch(3, compactInterval = 3))
    assert(false === isCompactionBatch(4, compactInterval = 3))
    assert(true === isCompactionBatch(5, compactInterval = 3))
  }

  test("nextCompactionBatchId") {
    assert(2 === nextCompactionBatchId(0, compactInterval = 3))
    assert(2 === nextCompactionBatchId(1, compactInterval = 3))
    assert(5 === nextCompactionBatchId(2, compactInterval = 3))
    assert(5 === nextCompactionBatchId(3, compactInterval = 3))
    assert(5 === nextCompactionBatchId(4, compactInterval = 3))
    assert(8 === nextCompactionBatchId(5, compactInterval = 3))
  }

  test("getValidBatchesBeforeCompactionBatch") {
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(0, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(1, compactInterval = 3)
    }
    assert(Seq(0, 1) === getValidBatchesBeforeCompactionBatch(2, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(3, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(4, compactInterval = 3)
    }
    assert(Seq(2, 3, 4) === getValidBatchesBeforeCompactionBatch(5, compactInterval = 3))
  }

  test("getAllValidBatches") {
    assert(Seq(0) === getAllValidBatches(0, compactInterval = 3))
    assert(Seq(0, 1) === getAllValidBatches(1, compactInterval = 3))
    assert(Seq(2) === getAllValidBatches(2, compactInterval = 3))
    assert(Seq(2, 3) === getAllValidBatches(3, compactInterval = 3))
    assert(Seq(2, 3, 4) === getAllValidBatches(4, compactInterval = 3))
    assert(Seq(5) === getAllValidBatches(5, compactInterval = 3))
    assert(Seq(5, 6) === getAllValidBatches(6, compactInterval = 3))
    assert(Seq(5, 6, 7) === getAllValidBatches(7, compactInterval = 3))
    assert(Seq(8) === getAllValidBatches(8, compactInterval = 3))
  }

/*
  test("compactLogs") {
    withFileStreamSinkLog { sinkLog =>
      val logs = Seq(
        newFakeSinkFileStatus("/a/b/x", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/y", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/z", FileStreamSinkLog.ADD_ACTION))
      assert(logs === sinkLog.compactLogs(logs))

      val logs2 = Seq(
        newFakeSinkFileStatus("/a/b/m", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/n", FileStreamSinkLog.ADD_ACTION),
        newFakeSinkFileStatus("/a/b/z", FileStreamSinkLog.DELETE_ACTION))
      assert(logs.dropRight(1) ++ logs2.dropRight(1) === sinkLog.compactLogs(logs ++ logs2))
    }
  }

  test("serialize") {
    withFileStreamSinkLog { sinkLog =>
      val logs = Array(
        SinkFileStatus(
          path = "/a/b/x",
          size = 100L,
          isDir = false,
          modificationTime = 1000L,
          blockReplication = 1,
          blockSize = 10000L,
          action = FileStreamSinkLog.ADD_ACTION),
        SinkFileStatus(
          path = "/a/b/y",
          size = 200L,
          isDir = false,
          modificationTime = 2000L,
          blockReplication = 2,
          blockSize = 20000L,
          action = FileStreamSinkLog.DELETE_ACTION),
        SinkFileStatus(
          path = "/a/b/z",
          size = 300L,
          isDir = false,
          modificationTime = 3000L,
          blockReplication = 3,
          blockSize = 30000L,
          action = FileStreamSinkLog.ADD_ACTION))

      // scalastyle:off
      val expected = s"""$VERSION
          |{"path":"/a/b/x","size":100,"isDir":false,"modificationTime":1000,"blockReplication":1,"blockSize":10000,"action":"add"}
          |{"path":"/a/b/y","size":200,"isDir":false,"modificationTime":2000,"blockReplication":2,"blockSize":20000,"action":"delete"}
          |{"path":"/a/b/z","size":300,"isDir":false,"modificationTime":3000,"blockReplication":3,"blockSize":30000,"action":"add"}""".stripMargin
      // scalastyle:on
      val baos = new ByteArrayOutputStream()
      sinkLog.serialize(logs, baos)
      assert(expected === baos.toString(UTF_8.name()))
      baos.reset()
      sinkLog.serialize(Array(), baos)
      assert(VERSION === baos.toString(UTF_8.name()))
    }
  }

  test("deserialize") {
    withFileStreamSinkLog { sinkLog =>
      // scalastyle:off
      val logs = s"""$VERSION
          |{"path":"/a/b/x","size":100,"isDir":false,"modificationTime":1000,"blockReplication":1,"blockSize":10000,"action":"add"}
          |{"path":"/a/b/y","size":200,"isDir":false,"modificationTime":2000,"blockReplication":2,"blockSize":20000,"action":"delete"}
          |{"path":"/a/b/z","size":300,"isDir":false,"modificationTime":3000,"blockReplication":3,"blockSize":30000,"action":"add"}""".stripMargin
      // scalastyle:on

      val expected = Seq(
        SinkFileStatus(
          path = "/a/b/x",
          size = 100L,
          isDir = false,
          modificationTime = 1000L,
          blockReplication = 1,
          blockSize = 10000L,
          action = FileStreamSinkLog.ADD_ACTION),
        SinkFileStatus(
          path = "/a/b/y",
          size = 200L,
          isDir = false,
          modificationTime = 2000L,
          blockReplication = 2,
          blockSize = 20000L,
          action = FileStreamSinkLog.DELETE_ACTION),
        SinkFileStatus(
          path = "/a/b/z",
          size = 300L,
          isDir = false,
          modificationTime = 3000L,
          blockReplication = 3,
          blockSize = 30000L,
          action = FileStreamSinkLog.ADD_ACTION))

      assert(expected === sinkLog.deserialize(new ByteArrayInputStream(logs.getBytes(UTF_8))))

      assert(Nil === sinkLog.deserialize(new ByteArrayInputStream(VERSION.getBytes(UTF_8))))
    }
  }

  test("batchIdToPath") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withFileStreamSinkLog { sinkLog =>
        assert("0" === sinkLog.batchIdToPath(0).getName)
        assert("1" === sinkLog.batchIdToPath(1).getName)
        assert("2.compact" === sinkLog.batchIdToPath(2).getName)
        assert("3" === sinkLog.batchIdToPath(3).getName)
        assert("4" === sinkLog.batchIdToPath(4).getName)
        assert("5.compact" === sinkLog.batchIdToPath(5).getName)
      }
    }
  }

  testWithUninterruptibleThread("compact") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withFileStreamSinkLog { sinkLog =>
        for (batchId <- 0 to 10) {
          sinkLog.add(
            batchId,
            Array(newFakeSinkFileStatus("/a/b/" + batchId, FileStreamSinkLog.ADD_ACTION)))
          val expectedFiles = (0 to batchId).map {
            id => newFakeSinkFileStatus("/a/b/" + id, FileStreamSinkLog.ADD_ACTION)
          }
          assert(sinkLog.allFiles() === expectedFiles)
          if (isCompactionBatch(batchId, 3)) {
            // Since batchId is a compaction batch, the batch log file should contain all logs
            assert(sinkLog.get(batchId).getOrElse(Nil) === expectedFiles)
          }
        }
      }
    }
  }

  testWithUninterruptibleThread("delete expired file") {
    // Set FILE_SINK_LOG_CLEANUP_DELAY to 0 so that we can detect the deleting behaviour
    // deterministically
    withSQLConf(
      SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3",
      SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.key -> "0") {
      withFileStreamSinkLog { sinkLog =>
        val fs = sinkLog.metadataPath.getFileSystem(spark.sessionState.newHadoopConf())

        def listBatchFiles(): Set[String] = {
          fs.listStatus(sinkLog.metadataPath).map(_.getPath.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        sinkLog.add(0, Array(newFakeSinkFileStatus("/a/b/0", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0") === listBatchFiles())
        sinkLog.add(1, Array(newFakeSinkFileStatus("/a/b/1", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("0", "1") === listBatchFiles())
        sinkLog.add(2, Array(newFakeSinkFileStatus("/a/b/2", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact") === listBatchFiles())
        sinkLog.add(3, Array(newFakeSinkFileStatus("/a/b/3", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3") === listBatchFiles())
        sinkLog.add(4, Array(newFakeSinkFileStatus("/a/b/4", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        sinkLog.add(5, Array(newFakeSinkFileStatus("/a/b/5", FileStreamSinkLog.ADD_ACTION)))
        assert(Set("5.compact") === listBatchFiles())
      }
    }
  }


  testWithUninterruptibleThread("HDFSMetadataLog: basic") {
    withTempDir { temp =>
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
      val metadataLog = new FakeCompactibleFileStreamLog("vvv", spark, dir.getAbsolutePath)
      assert(metadataLog.add(0, Array("batch0")))
      val x = metadataLog.getLatest();
      x
      val y: Tuple2[Int, Int] = Tuple2(1, 2)
      y.equals(2)
      assert((1, "b") === (1,"b"))
      assert(metadataLog.getLatest() === Some(0 -> Array("batch0")))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(None, Some(0)) === Array(0 -> "batch0"))

      assert(metadataLog.add(1, Array("batch1")))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))

      // Adding the same batch does nothing
      metadataLog.add(1, Array("batch1-duplicated"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }
  */
/*
  testWithUninterruptibleThread(
    "HDFSMetadataLog: fallback from FileContext to FileSystem", quietly = true) {
    spark.conf.set(
      s"fs.$scheme.impl",
      classOf[FakeFileSystem].getName)
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, s"$scheme://$temp")
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(None, Some(0)) === Array(0 -> "batch0"))


      val metadataLog2 = new HDFSMetadataLog[String](spark, s"$scheme://$temp")
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog2.get(None, Some(0)) === Array(0 -> "batch0"))

    }
  }

  testWithUninterruptibleThread("HDFSMetadataLog: purge") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.add(2, "batch2"))
      assert(metadataLog.get(0).isDefined)
      assert(metadataLog.get(1).isDefined)
      assert(metadataLog.get(2).isDefined)
      assert(metadataLog.getLatest().get._1 == 2)

      metadataLog.purge(2)
      assert(metadataLog.get(0).isEmpty)
      assert(metadataLog.get(1).isEmpty)
      assert(metadataLog.get(2).isDefined)
      assert(metadataLog.getLatest().get._1 == 2)

      // There should be exactly one file, called "2", in the metadata directory.
      // This check also tests for regressions of SPARK-17475
      val allFiles = new File(metadataLog.metadataPath.toString).listFiles().toSeq
      assert(allFiles.size == 1)
      assert(allFiles(0).getName() == "2")
    }
  }

  testWithUninterruptibleThread("HDFSMetadataLog: restart") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))

      val metadataLog2 = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.get(1) === Some("batch1"))
      assert(metadataLog2.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog2.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }

  test("HDFSMetadataLog: metadata directory collision") {
    withTempDir { temp =>
      val waiter = new Waiter
      val maxBatchId = 100
      for (id <- 0 until 10) {
        new UninterruptibleThread(s"HDFSMetadataLog: metadata directory collision - thread $id") {
          override def run(): Unit = waiter {
            val metadataLog =
              new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
            try {
              var nextBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)
              nextBatchId += 1
              while (nextBatchId <= maxBatchId) {
                metadataLog.add(nextBatchId, nextBatchId.toString)
                nextBatchId += 1
              }
            } catch {
              case e: ConcurrentModificationException =>
              // This is expected since there are multiple writers
            } finally {
              waiter.dismiss()
            }
          }
        }.start()
      }

      waiter.await(timeout(10.seconds), dismissals(10))
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.getLatest() === Some(maxBatchId -> maxBatchId.toString))
      assert(
        metadataLog.get(None, Some(maxBatchId)) === (0 to maxBatchId).map(i => (i, i.toString)))
    }
  }

  /** Basic test case for [[FileManager]] implementation. */
  private def testFileManager(basePath: Path, fm: FileManager): Unit = {
    // Mkdirs
    val dir = new Path(s"$basePath/dir/subdir/subsubdir")
    assert(!fm.exists(dir))
    fm.mkdirs(dir)
    assert(fm.exists(dir))
    fm.mkdirs(dir)

    // List
    val acceptAllFilter = new PathFilter {
      override def accept(path: Path): Boolean = true
    }
    val rejectAllFilter = new PathFilter {
      override def accept(path: Path): Boolean = false
    }
    assert(fm.list(basePath, acceptAllFilter).exists(_.getPath.getName == "dir"))
    assert(fm.list(basePath, rejectAllFilter).length === 0)

    // Create
    val path = new Path(s"$dir/file")
    assert(!fm.exists(path))
    fm.create(path).close()
    assert(fm.exists(path))
    intercept[IOException] {
      fm.create(path)
    }

    // Open and delete
    val f1 = fm.open(path)
    fm.delete(path)
    assert(!fm.exists(path))
    intercept[IOException] {
      fm.open(path)
    }
    fm.delete(path)  // should not throw exception
    f1.close()

    // Rename
    val path1 = new Path(s"$dir/file1")
    val path2 = new Path(s"$dir/file2")
    fm.create(path1).close()
    assert(fm.exists(path1))
    fm.rename(path1, path2)
    intercept[FileNotFoundException] {
      fm.rename(path1, path2)
    }
    val path3 = new Path(s"$dir/file3")
    fm.create(path3).close()
    assert(fm.exists(path3))
    intercept[FileAlreadyExistsException] {
      fm.rename(path2, path3)
    }
  }*/
}

class FakeCompactibleFileStreamLog(
                                       metadataLogVersion: String,
                                       sparkSession: SparkSession,
                                       path: String)
  extends CompactibleFileStreamLog[String] (metadataLogVersion, sparkSession, path) {

  override protected def fileCleanupDelayMs: Long = 100

  override protected def isDeletingExpiredLog: Boolean = true

  override protected def compactInterval: Int = 3

  override protected def serializeData(t: String): String = t

  override protected def deserializeData(encodedString: String): String = encodedString

  override def compactLogs(logs: Seq[String]): Seq[String] = logs
}

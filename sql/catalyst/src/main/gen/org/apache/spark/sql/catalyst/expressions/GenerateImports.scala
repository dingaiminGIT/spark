package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass

object GenerateImports extends App {

  // Same as GodeGenerator.scala 's imports
  // https://github.com/apache/spark/blame/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeGenerator.scala

  import java.io.ByteArrayInputStream
  import java.util.{Map => JavaMap}

  import scala.collection.JavaConverters._
  import scala.collection.mutable
  import scala.collection.mutable.ArrayBuffer

  import com.google.common.cache.{CacheBuilder, CacheLoader}
  import org.codehaus.commons.compiler.CompileException
  import org.codehaus.janino.{ByteArrayClassLoader, ClassBodyEvaluator, SimpleCompiler}
  import org.codehaus.janino.util.ClassFile
  import scala.language.existentials

  import org.apache.spark.SparkEnv
  import org.apache.spark.internal.Logging
  import org.apache.spark.metrics.source.CodegenMetrics
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions._
  import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
  import org.apache.spark.sql.types._
  import org.apache.spark.unsafe.Platform
  import org.apache.spark.unsafe.types._
  import org.apache.spark.util.{ParentClassLoader, Utils}

  // same as GodeGenerator.scala 's evaluator.setDefaultImports
  val names = Array(
    classOf[Platform].getName,
    classOf[InternalRow].getName,
    classOf[UnsafeRow].getName,
    classOf[UTF8String].getName,
    classOf[Decimal].getName,
    classOf[CalendarInterval].getName,
    classOf[ArrayData].getName,
    classOf[UnsafeArrayData].getName,
    classOf[MapData].getName,
    classOf[UnsafeMapData].getName,
    classOf[MutableRow].getName,
    classOf[Expression].getName,

    // extends GeneratedClass
    classOf[GeneratedClass].getName)

  println(names.mkString("import ", ";\nimport ", ";"))
}

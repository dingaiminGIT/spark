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

package org.apache.spark.streaming.ui

import org.apache.spark.SparkFunSuite
import org.scalatest.Matchers

class StreamingPageSuite extends SparkFunSuite with Matchers {

  test("divideIntoSegmentsByMaxY()") {

    import StreamingPage.divideIntoSegmentsByMaxY

    val MAX_Y = 100.0D
    var input: Seq[(Long, Double)] = null
    var expected: Seq[Seq[(Long, Double)]] = null

    input = Seq(
      (1L, MAX_Y), (2L, MAX_Y),
      (3L, 1.0), (4L, 2.0), (5L, MAX_Y), (6L, 3.0),
      (7L, MAX_Y), (8L, MAX_Y), (9L, MAX_Y), (10L, MAX_Y),
      (11L, 1.0), (12L, 2.0), (13L, MAX_Y), (14L, 3.0), (15L, 3.0),
      (16L, MAX_Y), (17L, MAX_Y), (18L, MAX_Y), (19L, MAX_Y),
      (20L, 3.0)
    )
    expected = Seq(
      Seq((1L, MAX_Y), (2L, MAX_Y)),
      Seq((2L, MAX_Y), (3L, 1.0), (4L, 2.0), (5L, MAX_Y), (6L, 3.0), (7L, MAX_Y)),
      Seq((7L, MAX_Y), (8L, MAX_Y), (9L, MAX_Y), (10L, MAX_Y)),
      Seq((10L, MAX_Y), (11L, 1.0), (12L, 2.0), (13L, MAX_Y), (14L, 3.0), (15L, 3.0), (16L, MAX_Y)),
      Seq((16L, MAX_Y), (17L, MAX_Y), (18L, MAX_Y), (19L, MAX_Y)),
      Seq((19L, MAX_Y), (20L, 3.0))
    )
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

    input = Seq()
    expected = Seq(Seq())
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

    input = Seq((1L, MAX_Y))
    expected = Seq(Seq((1L, MAX_Y)))
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

    input = Seq((1L, 2.0))
    expected = Seq(Seq((1L, 2.0D)))
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

    input = Seq((1L, MAX_Y), (2L, MAX_Y))
    expected = Seq(Seq((1L, MAX_Y), (2L, MAX_Y)))
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

    input = Seq((1L, MAX_Y), (2L, MAX_Y), (3L, MAX_Y), (4L, MAX_Y), (5L, MAX_Y))
    expected = Seq(Seq((1L, MAX_Y), (2L, MAX_Y), (3L, MAX_Y), (4L, MAX_Y), (5L, MAX_Y)))
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

    input = Seq((1L, 2.0), (2L, 2.0))
    expected = Seq(
      Seq(),
      Seq((1L, 2.0), (2L, 2.0))
    )
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

    input = Seq((1L, 2.0), (2L, 2.0), (3L, 2.0), (4L, 2.0), (5L, 2.0))
    expected = Seq(
      Seq(),
      Seq((1L, 2.0), (2L, 2.0), (3L, 2.0), (4L, 2.0), (5L, 2.0))
    )
    divideIntoSegmentsByMaxY(input, MAX_Y) should be (expected)

  }
}

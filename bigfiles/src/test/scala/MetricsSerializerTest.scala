/**
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import za.co.absa.MetricsSerializer
import org.scalatest.funsuite.AnyFunSuite
import org.json4s._
import org.json4s.native.JsonMethods._
import za.co.absa.analysis.ComparisonMetrics

class MetricsSerializerTest extends AnyFunSuite {

  implicit val formats: DefaultFormats.type = DefaultFormats

  test("test that serialize returns correct JSON string for datasets with differences") {
    val metrics = ComparisonMetrics(
      rowCountA = 2,
      rowCountB = 3,
      columnCountA = 2,
      columnCountB = 2,
      diffCountA = 1,
      diffCountB = 2,
      uniqueRowCountA = 2,
      uniqueRowCountB = 3,
      sameRecordsCount = 1,
      sameRecordsPercentToA = 50.0,
      excludedColumns = Seq()
    )

    val result = MetricsSerializer.serialize(metrics)
    val expected = "{\"A\":{\"row count\":2," +
                     "\"column count\":2," +
                     "\"rows not present in B\":1," +
                     "\"unique rows count\":2}," +
                   "\"B\":{\"row count\":3," +
                     "\"column count\":2," +
                     "\"rows not present in A\":2," +
                     "\"unique rows count\":3}," +
                   "\"general\":{\"same records count\":1,\"same records percent to A\":50.0,\"excluded columns\":\"\"}}"

    assert(result == expected)
  }

  test("test that serialize returns correct JSON string with duplicates in data") {
    val metrics = ComparisonMetrics(
      rowCountA = 3,
      rowCountB = 4,
      columnCountA = 2,
      columnCountB = 2,
      diffCountA = 0,
      diffCountB = 1,
      uniqueRowCountA = 2,
      uniqueRowCountB = 2,
      sameRecordsCount = 3,
      sameRecordsPercentToA = 100.0,
      excludedColumns = Seq()
    )

    val result = MetricsSerializer.serialize(metrics)
    val expected = "{\"A\":{\"row count\":3," +
      "\"column count\":2," +
      "\"rows not present in B\":0," +
      "\"unique rows count\":2}," +
      "\"B\":{\"row count\":4," +
      "\"column count\":2," +
      "\"rows not present in A\":1," +
      "\"unique rows count\":2}," +
      "\"general\":{\"same records count\":3,\"same records percent to A\":100.0,\"excluded columns\":\"\"}}"

    assert(result == expected)
  }

  test("test that serialize returns correct JSON string with excluded columns") {
    val metrics = ComparisonMetrics(
      rowCountA = 2,
      rowCountB = 3,
      columnCountA = 1,
      columnCountB = 1,
      diffCountA = 0,
      diffCountB = 1,
      uniqueRowCountA = 2,
      uniqueRowCountB = 3,
      sameRecordsCount = 2,
      sameRecordsPercentToA = 100.0,
      excludedColumns = Seq("id")
    )

    val result = MetricsSerializer.serialize(metrics)
    val expected = "{\"A\":{\"row count\":2," +
      "\"column count\":1," +
      "\"rows not present in B\":0," +
      "\"unique rows count\":2}," +
      "\"B\":{\"row count\":3," +
      "\"column count\":1," +
      "\"rows not present in A\":1," +
      "\"unique rows count\":3}," +
      "\"general\":{\"same records count\":2,\"same records percent to A\":100.0,\"excluded columns\":\"id\"}}"

    assert(result == expected)
  }

  test("test that toJson creates parseable JSON object") {
    val metrics = ComparisonMetrics(
      rowCountA = 10,
      rowCountB = 12,
      columnCountA = 4,
      columnCountB = 4,
      diffCountA = 2,
      diffCountB = 4,
      uniqueRowCountA = 9,
      uniqueRowCountB = 11,
      sameRecordsCount = 8,
      sameRecordsPercentToA = 80.0,
      excludedColumns = Seq("timestamp", "id")
    )

    val json = MetricsSerializer.toJson(metrics)

    // Verify structure
    assert(json \ "A" != JNothing)
    assert(json \ "B" != JNothing)
    assert(json \ "general" != JNothing)

    // Verify A values
    assert((json \ "A" \ "row count").extract[Long] == 10)
    assert((json \ "A" \ "column count").extract[Int] == 4)
    assert((json \ "A" \ "rows not present in B").extract[Long] == 2)
    assert((json \ "A" \ "unique rows count").extract[Long] == 9)

    // Verify B values
    assert((json \ "B" \ "row count").extract[Long] == 12)
    assert((json \ "B" \ "column count").extract[Int] == 4)
    assert((json \ "B" \ "rows not present in A").extract[Long] == 4)
    assert((json \ "B" \ "unique rows count").extract[Long] == 11)

    // Verify general values
    assert((json \ "general" \ "same records count").extract[Long] == 8)
    assert((json \ "general" \ "same records percent to A").extract[Double] == 80.0)
    assert((json \ "general" \ "excluded columns").extract[String] == "timestamp, id")
  }
}


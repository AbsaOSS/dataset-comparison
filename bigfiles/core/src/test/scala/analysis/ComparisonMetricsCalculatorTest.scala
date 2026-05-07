package analysis

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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import testutil.SparkTestSession
import za.co.absa.analysis.ComparisonMetricsCalculator

class ComparisonMetricsCalculatorTest extends AnyFunSuite {
  implicit val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  test("test that calculator returns metrics when datasets are identical") {
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val diffA = Seq.empty[(Int, String)].toDF("id", "value")
    val diffB = Seq.empty[(Int, String)].toDF("id", "value")
    val metrics = ComparisonMetricsCalculator.calculate(tmp1, tmp2, diffA, diffB, Seq())

    assert(metrics.rowCountA == 2)
    assert(metrics.rowCountB == 2)
    assert(metrics.columnCountA == 2)
    assert(metrics.columnCountB == 2)
    assert(metrics.diffCountA == 0)
    assert(metrics.diffCountB == 0)
    assert(metrics.uniqueRowCountA == 2)
    assert(metrics.uniqueRowCountB == 2)
    assert(metrics.sameRecordsCount == 2)
    assert(metrics.sameRecordsPercentToA == 100.0)
    assert(metrics.excludedColumns.isEmpty)
  }

  test("test that calculator returns correct metrics when datasets differ") {
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "three"), (3, "two")).toDF("id", "value")

    val diffA = Seq((2, "two")).toDF("id", "value")
    val diffB = Seq((2, "three"), (3, "two")).toDF("id", "value")
    val metrics = ComparisonMetricsCalculator.calculate(tmp1, tmp2, diffA, diffB, Seq())

    assert(metrics.rowCountA == 2)
    assert(metrics.rowCountB == 3)
    assert(metrics.columnCountA == 2)
    assert(metrics.columnCountB == 2)
    assert(metrics.diffCountA == 1)
    assert(metrics.diffCountB == 2)
    assert(metrics.uniqueRowCountA == 2)
    assert(metrics.uniqueRowCountB == 3)
    assert(metrics.sameRecordsCount == 1)
    assert(metrics.sameRecordsPercentToA == 50.0)
    assert(metrics.excludedColumns.isEmpty)
  }

  test("test that calculator returns correct metrics with duplicates in data") {
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (1, "one"), (1, "one"), (2, "two")).toDF("id", "value")

    val diffA = Seq.empty[(Int, String)].toDF("id", "value")
    val diffB = Seq((1, "one")).toDF("id", "value")
    val metrics = ComparisonMetricsCalculator.calculate(tmp1, tmp2, diffA, diffB, Seq())

    assert(metrics.rowCountA == 3)
    assert(metrics.rowCountB == 4)
    assert(metrics.columnCountA == 2)
    assert(metrics.columnCountB == 2)
    assert(metrics.diffCountA == 0)
    assert(metrics.diffCountB == 1)
    assert(metrics.uniqueRowCountA == 2)
    assert(metrics.uniqueRowCountB == 2)
    assert(metrics.sameRecordsCount == 3)
    assert(metrics.sameRecordsPercentToA == 100.0)
    assert(metrics.excludedColumns.isEmpty)
  }

  test("test that calculator returns correct metrics with excluded columns") {
    val tmp1: DataFrame = Seq(("one"), ("two")).toDF("value")
    val tmp2: DataFrame = Seq(("one"), ("three"), ("two")).toDF("value")

    val diffA = Seq.empty[String].toDF("value")
    val diffB = Seq("three").toDF("value")
    val metrics = ComparisonMetricsCalculator.calculate(tmp1, tmp2, diffA, diffB, Seq("id"))

    assert(metrics.rowCountA == 2)
    assert(metrics.rowCountB == 3)
    assert(metrics.columnCountA == 1)
    assert(metrics.columnCountB == 1)
    assert(metrics.diffCountA == 0)
    assert(metrics.diffCountB == 1)
    assert(metrics.uniqueRowCountA == 2)
    assert(metrics.uniqueRowCountB == 3)
    assert(metrics.sameRecordsCount == 2)
    assert(metrics.sameRecordsPercentToA == 100.0)
    assert(metrics.excludedColumns == Seq("id"))
  }

  test("test that calculator returns metrics when datasets are completely different") {
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq(("12af", 1003), ("12qw", 3004), ("123q", 3456)).toDF("id", "value")

    val diffA = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val diffB = Seq(("12af", 1003), ("12qw", 3004), ("123q", 3456)).toDF("id", "value")
    val metrics = ComparisonMetricsCalculator.calculate(tmp1, tmp2, diffA, diffB, Seq())

    assert(metrics.rowCountA == 2)
    assert(metrics.rowCountB == 3)
    assert(metrics.diffCountA == 2)
    assert(metrics.diffCountB == 3)
    assert(metrics.sameRecordsCount == 0)
    assert(metrics.sameRecordsPercentToA == 0.0)
  }

  test("test that excluded columns are reflected correctly in result") {
    val tmp1: DataFrame = Seq((1, "one", 100), (2, "two", 200)).toDF("id", "value", "extra")
    val tmp2: DataFrame = Seq((1, "one", 999), (2, "two", 888)).toDF("id", "value", "extra")

    val diffA = Seq.empty[(Int, String, Int)].toDF("id", "value", "extra")
    val diffB = Seq.empty[(Int, String, Int)].toDF("id", "value", "extra")
    val metrics = ComparisonMetricsCalculator.calculate(tmp1, tmp2, diffA, diffB, Seq("extra"))

    assert(metrics.diffCountA == 0)
    assert(metrics.diffCountB == 0)
    assert(metrics.sameRecordsCount == 2)
    assert(metrics.sameRecordsPercentToA == 100.0)
    assert(metrics.excludedColumns == Seq("extra"))
  }
}

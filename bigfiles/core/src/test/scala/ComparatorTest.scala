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

import za.co.absa.Comparator
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import testutil.SparkTestSession

class ComparatorTest extends AnyFunSuite {
  implicit val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  test("test that comparator returns diff rows"){
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "three"), (3, "two")).toDF("id", "value")

    val (diffA, diffB) = Comparator.compare(tmp1, tmp2)
    assert(diffA.count() == 1)
    assert(diffB.count() == 2)
  }

  test("test that comparator returns all rows if dataframes are completely different"){
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq(("12af", 1003), ("12qw", 3004), ("123q", 3456)).toDF("id", "value")

    val (diffA, diffB) = Comparator.compare(tmp1, tmp2)
    assert(diffA.count() == 2)
    assert(diffB.count() == 3)
  }

  test("test that comparator returns empty DataFrames if all rows are the same"){
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diffA, diffB) = Comparator.compare(tmp1, tmp2)
    assert(diffA.count() == 0)
    assert(diffB.count() == 0)
  }

  test("test that comparator returns empty DataFrames if all rows are the same but in different order"){
    val tmp1: DataFrame = Seq((2, "two"), (1, "one")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diffA, diffB) = Comparator.compare(tmp1, tmp2)
    assert(diffA.count() == 0)
    assert(diffB.count() == 0)
  }

  test("test that comparator returns correct dataframes if one duplicate is present in one table"){
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diffA, diffB) = Comparator.compare(tmp1, tmp2)

    assert(diffA.count() == 1)
    assert(diffB.count() == 0)
  }

  test("test that comparator returns correct dataframes if 2 duplicates are present in one table"){
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diffA, diffB) = Comparator.compare(tmp1, tmp2)

    assert(diffA.count() == 2)
    assert(diffB.count() == 0)
  }


  test("test that comparator returns correct dataframes if duplicates are present"){
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val (diffA, diffB) = Comparator.compare(tmp1, tmp2)

    assert(diffA.count() == 0)
    assert(diffB.count() == 1)
  }

  test("test that comparator excludes columns correctly") {
    val tmp1: DataFrame = Seq((1, "one", 100), (2, "two", 200)).toDF("id", "value", "extra")
    val tmp2: DataFrame = Seq((1, "one", 999), (2, "two", 888)).toDF("id", "value", "extra")

    // Without exclusion, datasets differ
    val (diffA1, diffB1) = Comparator.compare(tmp1, tmp2)
    assert(diffA1.count() == 2)
    assert(diffB1.count() == 2)

    // With exclusion of "extra" column, datasets are identical
    val (diffA2, diffB2) = Comparator.compare(tmp1, tmp2, Seq("extra"))
    assert(diffA2.count() == 0)
    assert(diffB2.count() == 0)
  }

  test("test that comparator excludes multiple columns correctly") {
    val tmp1: DataFrame = Seq((1, "one", 100, "x"), (2, "two", 200, "y")).toDF("id", "value", "extra1", "extra2")
    val tmp2: DataFrame = Seq((1, "one", 999, "z"), (2, "two", 888, "w")).toDF("id", "value", "extra1", "extra2")

    // Excluding both extra columns makes datasets identical
    val (diffA, diffB) = Comparator.compare(tmp1, tmp2, Seq("extra1", "extra2"))
    assert(diffA.count() == 0)
    assert(diffB.count() == 0)
  }

}

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

import za.co.absa.analysis.{AnalysisResult, ColumnsDiff, RowByRowAnalysis, RowsDiff}
import za.co.absa.analysis.RowByRowAnalysis.generateDiffJson
import za.co.absa.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import testutil.SparkTestSession
import upickle.default._


class RowByRowAnalysesTest extends AnyFunSuite{
  implicit val spark: SparkSession = SparkTestSession.spark
  import spark.implicits._
  implicit val ColumnsDiffRw: ReadWriter[ColumnsDiff] = macroRW
  implicit val RowDiffRw: ReadWriter[RowsDiff] = macroRW

  test("test analyses multiple changes") {
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "b", 3.0, 19830),
      (2, "b", 4.5, 29845),
      (3, "c", 3.0, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val rowsDiffListA = generateDiffJson(dataA, dataB, "A")
    val rowsDiffListB = generateDiffJson(dataB, dataA, "B")


    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "11133", inputRightHash = "19830", diffs = List(ColumnsDiff(columnName = "name", values = List("a", "b"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "49840", inputRightHash = "29845", diffs = List(
        ColumnsDiff(columnName = "id", values = List("4", "2")),
        ColumnsDiff(columnName = "value", values = List("4.0", "4.5")))))
      ||
      rowsDiffListA.contains(RowsDiff(inputLeftHash = "49840", inputRightHash = "19830", diffs = List(
        ColumnsDiff(columnName = "id", values = List("4", "1")),
        ColumnsDiff(columnName = "value", values = List("4.0", "3.0")))))
    )
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "39950", inputRightHash = "39955", diffs = List(ColumnsDiff(columnName = "value", values = List("5.0", "3.0"))))))


    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "19830", inputRightHash = "11133", diffs = List(ColumnsDiff(columnName = "name", values = List("b", "a"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "29845", inputRightHash = "49840", diffs = List(
      ColumnsDiff(columnName = "id", values = List("2", "4")),
      ColumnsDiff(columnName = "value", values = List("4.5", "4.0")))))
    )
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "39955", inputRightHash = "39950", diffs = List(ColumnsDiff(columnName = "value", values = List("3.0", "5.0"))))))


  }
  test("test analyses change in the same column") {
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "a", 3.5, 19830),
      (4, "b", 4.5, 29845),
      (3, "c", 5.5, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val rowsDiffListA = generateDiffJson(dataA, dataB, "A")
    val rowsDiffListB = generateDiffJson(dataB, dataA, "B")


    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "11133", inputRightHash = "19830", diffs = List(ColumnsDiff(columnName = "value", values = List("3.0", "3.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "49840", inputRightHash = "29845", diffs = List(ColumnsDiff(columnName = "value", values = List("4.0", "4.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "39950", inputRightHash = "39955", diffs = List(ColumnsDiff(columnName = "value", values = List("5.0", "5.5"))))))


    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "19830", inputRightHash = "11133", diffs = List(ColumnsDiff(columnName = "value", values = List("3.5", "3.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "29845", inputRightHash = "49840", diffs = List(ColumnsDiff(columnName = "value", values = List("4.5", "4.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "39955", inputRightHash = "39950", diffs = List(ColumnsDiff(columnName = "value", values = List("5.5", "5.0"))))))
  }

  test("test analyses one change in different columns"){
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "b", 3.0, 19830),
      (4, "b", 4.5, 29845),
      (4, "c", 5.0, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)


    val rowsDiffListA = generateDiffJson(dataA, dataB, "A")
    val rowsDiffListB = generateDiffJson(dataB, dataA, "B")


    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "11133", inputRightHash = "19830", diffs = List(ColumnsDiff(columnName = "name", values = List("a", "b"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "49840", inputRightHash = "29845", diffs = List(ColumnsDiff(columnName = "value", values = List("4.0", "4.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "39950", inputRightHash = "39955", diffs = List(ColumnsDiff(columnName = "id", values = List("3", "4"))))))


    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "19830", inputRightHash = "11133", diffs = List(ColumnsDiff(columnName = "name", values = List("b", "a"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "29845", inputRightHash = "49840", diffs = List(ColumnsDiff(columnName = "value", values = List("4.5", "4.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "39955", inputRightHash = "39950", diffs = List(ColumnsDiff(columnName = "id", values = List("4", "3"))))))
  }

  test("test analyses one Dataframe is smaller than the other"){
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "a", 3.5, 19830),
      (4, "b", 4.5, 29845),
      (3, "c", 5.5, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val rowsDiffListA = generateDiffJson(dataA, dataB, "A")
    val rowsDiffListB = generateDiffJson(dataB, dataA, "B")


    assert(rowsDiffListA.length == 2)
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "11133", inputRightHash = "19830", diffs = List(ColumnsDiff(columnName = "value", values = List("3.0", "3.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "49840", inputRightHash = "29845", diffs = List(ColumnsDiff(columnName = "value", values = List("4.0", "4.5"))))))


    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "19830", inputRightHash = "11133", diffs = List(ColumnsDiff(columnName = "value", values = List("3.5", "3.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "29845", inputRightHash = "49840", diffs = List(ColumnsDiff(columnName = "value", values = List("4.5", "4.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "39955", inputRightHash = "11133", diffs = List(
      ColumnsDiff(columnName = "id", values = List("3", "1")),
      ColumnsDiff(columnName = "name", values = List("c", "a")),
      ColumnsDiff(columnName = "value", values = List("5.5", "3.0"))))) ||
      rowsDiffListB.contains(RowsDiff(inputLeftHash = "39955", inputRightHash = "49840", diffs = List(
        ColumnsDiff(columnName = "id", values = List("3", "4")),
        ColumnsDiff(columnName = "name", values = List("c", "b")),
        ColumnsDiff(columnName = "value", values = List("5.5", "4.0")))))
    )

  }

  test("test analyses no changes"){
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val rowsDiffListA = generateDiffJson(dataA, dataB, "A")
    val rowsDiffListB = generateDiffJson(dataB, dataA, "B")

    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "11133", inputRightHash = "11133", diffs = List())))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "49840", inputRightHash = "49840", diffs = List())))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "39950", inputRightHash = "39950", diffs = List())))

    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "11133", inputRightHash = "11133", diffs = List())))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "49840", inputRightHash = "49840", diffs = List())))
    assert(rowsDiffListB.contains(RowsDiff(inputLeftHash = "39950", inputRightHash = "39950", diffs = List())))
  }



  // ============================================================================
  // Tests for analyze() method (AnalysisResult)
  // ============================================================================

  test("analyze returns DatasetsIdentical when datasets are identical") {
    val diffA: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)

    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold = 100)

    result match {
      case AnalysisResult.DatasetsIdentical =>
        assert(true, "Correctly identified identical datasets")
      case other =>
        fail(s"Expected DatasetsIdentical but got $other")
    }
  }

  test("analyze returns Success when differences are within threshold") {
    val diffA: DataFrame = Seq((2, "two", 12345)).toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq((2, "three", 67890)).toDF("id", "value", HASH_COLUMN_NAME)

    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold = 10)

    result match {
      case AnalysisResult.Success(diffAToB, diffBToA) =>
        assert(diffAToB.nonEmpty, "Should have differences from A to B")
        assert(diffBToA.nonEmpty, "Should have differences from B to A")
        assert(diffAToB.head.inputLeftHash.nonEmpty, "Should have hash for left row")
        assert(diffAToB.head.inputRightHash.nonEmpty, "Should have hash for right row")
      case other =>
        fail(s"Expected Success but got $other")
    }
  }

  test("analyze returns ThresholdExceeded when differences exceed threshold") {
    val diffA: DataFrame = Seq(
      (1, "one", 11111),
      (2, "two", 22222),
      (3, "three", 33333),
      (4, "four", 44444),
      (5, "five", 55555)
    ).toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq(
      (1, "ONE", 11112),
      (2, "TWO", 22223),
      (3, "THREE", 33334),
      (4, "FOUR", 44445),
      (5, "FIVE", 55556)
    ).toDF("id", "value", HASH_COLUMN_NAME)

    val threshold = 2
    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold)

    result match {
      case AnalysisResult.ThresholdExceeded(countA, countB, thresh) =>
        assert(countA == 5, s"Expected 5 differences in A but got $countA")
        assert(countB == 5, s"Expected 5 differences in B but got $countB")
        assert(thresh == threshold, s"Expected threshold $threshold but got $thresh")
      case other =>
        fail(s"Expected ThresholdExceeded but got $other")
    }
  }

  test("analyze returns OneSidedDifference when only A has differences even with low threshold") {
    val diffA: DataFrame = Seq((3, "three", 33333)).toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)

    val threshold = 0
    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold)

    result match {
      case AnalysisResult.OneSidedDifference(countA, countB) =>
        assert(countA == 1, s"Expected 1 difference in A but got $countA")
        assert(countB == 0, s"Expected 0 differences in B but got $countB")
      case other =>
        fail(s"Expected OneSidedDifference but got $other")
    }
  }

  test("analyze returns OneSidedDifference when only B has differences even with low threshold") {
    val diffA: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq((2, "two", 22222), (3, "three", 33333)).toDF("id", "value", HASH_COLUMN_NAME)

    val threshold = 1
    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold)

    result match {
      case AnalysisResult.OneSidedDifference(countA, countB) =>
        assert(countA == 0, s"Expected 0 differences in A but got $countA")
        assert(countB == 2, s"Expected 2 differences in B but got $countB")
      case other =>
        fail(s"Expected OneSidedDifference but got $other")
    }
  }

  test("analyze returns OneSidedDifference when only A has differences") {
    val diffA: DataFrame = Seq((3, "three", 33333)).toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)

    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold = 10)

    result match {
      case AnalysisResult.OneSidedDifference(countA, countB) =>
        assert(countA == 1, s"Expected 1 difference in A but got $countA")
        assert(countB == 0, s"Expected 0 differences in B but got $countB")
      case other =>
        fail(s"Expected OneSidedDifference but got $other")
    }
  }

  test("analyze returns OneSidedDifference when only B has differences") {
    val diffA: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq((3, "three", 33333), (4, "four", 44444)).toDF("id", "value", HASH_COLUMN_NAME)

    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold = 10)

    result match {
      case AnalysisResult.OneSidedDifference(countA, countB) =>
        assert(countA == 0, s"Expected 0 differences in A but got $countA")
        assert(countB == 2, s"Expected 2 differences in B but got $countB")
      case other =>
        fail(s"Expected OneSidedDifference but got $other")
    }
  }

  test("analyze returns Success with correct row diffs for small differences") {
    val diffA: DataFrame = Seq((1, "one", 100, 11111), (2, "two", 200, 22222)).toDF("id", "name", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq((1, "one", 999, 11112), (2, "two", 888, 22223)).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold = 10)

    result match {
      case AnalysisResult.Success(diffAToB, diffBToA) =>
        assert(diffAToB.length == 2, s"Expected 2 row diffs from A to B but got ${diffAToB.length}")
        assert(diffBToA.length == 2, s"Expected 2 row diffs from B to A but got ${diffBToA.length}")
        assert(diffAToB.forall(_.diffs.nonEmpty), "All row diffs should have column differences")
        assert(diffBToA.forall(_.diffs.nonEmpty), "All row diffs should have column differences")
      case other =>
        fail(s"Expected Success but got $other")
    }
  }

  test("analyze result is type-safe and can be pattern matched") {
    val diffA: DataFrame = Seq((1, "a", 11111)).toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = Seq((1, "b", 11112)).toDF("id", "value", HASH_COLUMN_NAME)

    val result: AnalysisResult = RowByRowAnalysis.analyze(diffA, diffB, threshold = 10)

    val message = result match {
      case AnalysisResult.Success(_, _) => "analysis complete"
      case AnalysisResult.DatasetsIdentical => "identical"
      case AnalysisResult.OneSidedDifference(_, _) => "one-sided"
      case AnalysisResult.ThresholdExceeded(_, _, _) => "too many diffs"
    }

    assert(message == "analysis complete")
  }

  test("analyze returns DatasetsIdentical for empty diff DataFrames") {
    val emptyDiffA: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)
    val emptyDiffB: DataFrame = Seq.empty[(Int, String, Int)].toDF("id", "value", HASH_COLUMN_NAME)

    val result = RowByRowAnalysis.analyze(emptyDiffA, emptyDiffB, threshold = 10)

    assert(result == AnalysisResult.DatasetsIdentical)
  }

  test("analyze ThresholdExceeded contains exact counts") {
    val diffA: DataFrame = (1 to 10).map(i => (i, s"value$i", 10000 + i)).toDF("id", "value", HASH_COLUMN_NAME)
    val diffB: DataFrame = (11 to 25).map(i => (i, s"value$i", 20000 + i)).toDF("id", "value", HASH_COLUMN_NAME)

    val result = RowByRowAnalysis.analyze(diffA, diffB, threshold = 5)

    result match {
      case AnalysisResult.ThresholdExceeded(countA, countB, thresh) =>
        assert(countA == 10, "Count A should be exactly 10")
        assert(countB == 15, "Count B should be exactly 15")
        assert(thresh == 5, "Threshold should be exactly 5")
      case other =>
        fail(s"Expected ThresholdExceeded but got $other")
    }
  }
}

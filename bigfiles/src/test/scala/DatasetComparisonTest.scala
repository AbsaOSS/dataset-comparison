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

import africa.absa.cps.DatasetComparison
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Paths
import scala.reflect.io.Directory



class DatasetComparisonTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter{
  implicit val spark: SparkSession = SparkTestSession.spark
  val folder: String = Paths.get("src/test/resources/").toAbsolutePath.toString
  val testOutput: String = folder + "/testoutput"


  import spark.implicits._

  val inputAResult : DataFrame = Seq(("Lisa", 27, "Madrid"), ("Luck", 33, "Geneve"), ("Marco", 47, "Rome"))
                                .toDF("Name", "Age", "City")

  val inputBResult : DataFrame = Seq(("Lisa", 20, "Madrid"), ("Luck", 30, "New York"), ("Marco", 47, "Geneve"))
                                .toDF("Name", "Age", "City")

  after {
    val dir = new Directory(new File(testOutput))
    dir.deleteRecursively()
  }


  test("test that DatasetComparison generates the correct output files") {
    val args = Array[String]("-o", testOutput, "--inputA", folder + "/namesA.parquet", "--inputB", folder + "/namesB.parquet")
    DatasetComparison.main(args)


    val inputADifferences = new File(testOutput + "/inputA_differences")
    val inputBDifferences = new File(testOutput + "/inputB_differences")
    val metricsJson = new File(testOutput + "/metrics.json")

    assert(inputADifferences.exists())
    assert(inputBDifferences.exists())
    assert(metricsJson.exists())

    val inputADifferencesDf = spark.read.parquet(inputADifferences.getAbsolutePath)
    val inputBDifferencesDf = spark.read.parquet(inputBDifferences.getAbsolutePath)
    val metricsDf = spark.read.json(metricsJson.getAbsolutePath)

    // check that inputADifferencesDf is the same as inputAResult
    val AClean = inputADifferencesDf.drop("cps_comparison_hash")
    assert(AClean.count() == inputAResult.count())
    assert(AClean.columns sameElements inputAResult.columns)
    assert(AClean.exceptAll(inputAResult).count() == 0)

    // check that inputBDifferencesDf is the same as inputBResult
    val BClean = inputBDifferencesDf.drop("cps_comparison_hash")
    assert(BClean.count() == inputBResult.count())
    assert(BClean.columns sameElements inputBResult.columns)
    assert(BClean.exceptAll(inputBResult).count() == 0)

    // check that metricsDf is not empty
    assert(metricsDf.count() > 0)
  }

  test("test that DatasetComparison generates the correct output files with --diff") {
    val args = Array[String]("-o", testOutput, "--inputA", folder + "/namesA.parquet", "--inputB", folder + "/namesB.parquet", "--diff", "Row")
    DatasetComparison.main(args)


    val inputADifferences = new File(testOutput + "/inputA_differences")
    val inputBDifferences = new File(testOutput + "/inputB_differences")
    val metricsJson = new File(testOutput + "/metrics.json")
    val AToBChanges = new File(testOutput + "/A_to_B_changes.json")
    val BToAChanges = new File(testOutput + "/B_to_A_changes.json")

    assert(inputADifferences.exists())
    assert(inputBDifferences.exists())
    assert(metricsJson.exists())
    assert(AToBChanges.exists())
    assert(BToAChanges.exists())

    val inputADifferencesDf = spark.read.parquet(inputADifferences.getAbsolutePath)
    val inputBDifferencesDf = spark.read.parquet(inputBDifferences.getAbsolutePath)
    val metricsDf = spark.read.json(metricsJson.getAbsolutePath)
    val AChangesToBDf = spark.read.json(AToBChanges.getAbsolutePath)
    val BChangesToADf = spark.read.json(BToAChanges.getAbsolutePath)

    // check that inputADifferencesDf is the same as inputAResult
    val AClean = inputADifferencesDf.drop("cps_comparison_hash")
    assert(AClean.exceptAll(inputAResult).count() == 0)

    // check that inputBDifferencesDf is the same as inputBResult
    val BClean = inputBDifferencesDf.drop("cps_comparison_hash")
    assert(BClean.exceptAll(inputBResult).count() == 0)

    // check that metricsDf is not empty
    assert(metricsDf.count() > 0)
    // check that AChangesToBDf is not empty
    assert(AChangesToBDf.count() > 0)
    // check that BChangesToADf is not empty
    assert(BChangesToADf.count() > 0)
  }

  test("test that DatasetComparison generates the correct output files with --format CSV") {
    val args = Array[String]("-o", testOutput, "--inputA", folder + "/namesA.parquet", "--inputB", folder + "/namesB.parquet", "--format", "csv")
    DatasetComparison.main(args)


    val inputADifferences = new File(testOutput + "/inputA_differences")
    val inputBDifferences = new File(testOutput + "/inputB_differences")
    val metricsJson = new File(testOutput + "/metrics.json")

    assert(inputADifferences.exists())
    assert(inputBDifferences.exists())
    assert(metricsJson.exists())

    val inputADifferencesDf = spark.read.option("header", "true").csv(inputADifferences.getAbsolutePath)
    val inputBDifferencesDf = spark.read.option("header", "true").csv(inputBDifferences.getAbsolutePath)
    val metricsDf = spark.read.json(metricsJson.getAbsolutePath)

    // check that inputADifferencesDf is the same as inputAResult
    val AClean = inputADifferencesDf.drop("cps_comparison_hash")
    assert(AClean.exceptAll(inputAResult).count() == 0)

    // check that inputBDifferencesDf is the same as inputBResult
    val BClean = inputBDifferencesDf.drop("cps_comparison_hash")
    assert(BClean.exceptAll(inputBResult).count() == 0)

    // check that metricsDf is not empty
    assert(metricsDf.count() > 0)
  }

}

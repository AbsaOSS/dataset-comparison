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

import za.co.absa.analysis.{ColumnsDiff, RowsDiff}
import za.co.absa.io.IOHandler
import za.co.absa.parser.OutputFormatType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.native.JsonMethods.{compact, render}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.json4s.JsonDSL._
import upickle.default._

import java.io.File
import java.nio.file.Paths
import scala.io.Source
import scala.reflect.io.Directory

class IOHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter{

  implicit val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._
  val folder = "testoutput"
  val filePath: String = folder + "/sample.parquet"
  val filePathCsv: String = folder + "/sample.csv"
  val jsonPath: String = folder +"/sample.json"


  after {
    // Clean up
    val dir = new Directory(new File("src/test/resources/" + folder))
    dir.deleteRecursively()
  }

  test("test that sparkRead reads a Parquet file into a DataFrame") {
    val tmp: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    tmp.write.mode(SaveMode.Overwrite).format("parquet").save(filePath)

    val df: DataFrame = IOHandler.sparkRead(filePath)

    assert(df != null)
    assert(df.count() == 2)
    assert(df.columns === Array("id", "value"))
  }

  test("test that dfWrite write a DataFrame to a Parquet file") {
    val df: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    IOHandler.dfWrite(filePath, df, OutputFormatType.Parquet)

    val writtenDf: DataFrame = spark.read.parquet(filePath)
    assert(writtenDf != null)
    assert(writtenDf.count() === 2)
    assert(writtenDf.columns === Array("id", "value"))

  }

  test("test that dfWrite write a DataFrame to a CSV file") {
    val df: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    IOHandler.dfWrite(filePathCsv, df, OutputFormatType.CSV)

    val writtenDf: DataFrame = spark.read.option("header", "true").csv(filePathCsv)
    assert(writtenDf != null)
    assert(writtenDf.count() === 2)
    assert(writtenDf.columns === Array("id", "value"))

  }

  test("test that jsonWrite write a DataFrame to a Json file") {
    val metricsJson =
      ("A" ->
        ("row count" -> 100) ~
        ("column count" -> 10) ~
        ("diff column count" -> 3)) ~
      ("B" ->
          ("row count" -> 102) ~
          ("column count" -> 10) ~
          ("diff column count" -> 5)) ~
      ("general" ->
          ("same records count" -> 97) ~
          ("same records percent" -> (math floor (97.0/100)*10000)/100))

    IOHandler.jsonWrite(Paths.get(jsonPath).toString, compact(render(metricsJson)))

    import org.json4s._
    import org.json4s.native.JsonMethods._
    val df = spark.read.json(jsonPath)
    val lines = df.toJSON.collect().mkString
    assert(parse(lines) === metricsJson)
  }

  test("test that rowDiffWriteAsJson writes a List[RowsDiff] to a JSON file") {
    implicit val ColumnsDiffRw: ReadWriter[ColumnsDiff] = macroRW
    implicit val RowDiffRw: ReadWriter[RowsDiff] = macroRW
    val data: List[RowsDiff] = List(
      RowsDiff(
        inputLeftHash = "hash1",
        inputRightHash = "hash2",
        diffs = List(
          ColumnsDiff(
            columnName = "id",
            values = List("1", "2")
          ),
          ColumnsDiff(
            columnName = "name",
            values = List("Alice", "Bob")
          )
        )
      ),
      RowsDiff(
        inputLeftHash = "hash3",
        inputRightHash = "hash4",
        diffs = List(
          ColumnsDiff(
            columnName = "id",
            values = List("3", "4")
          ),
          ColumnsDiff(
            columnName = "name",
            values = List("Charlie", "David")
          )
        )
      )
    )

    val dir = new File(folder)
    if (!dir.exists()) {
      dir.mkdirs()
    }

    IOHandler.rowDiffWriteAsJson(Paths.get(jsonPath).toString, data)

    val source = Source.fromFile(jsonPath)
    val jsonString = try source.mkString finally source.close()
    val rowsDiffListA: List[RowsDiff] = read[List[RowsDiff]](jsonString)
    assert(rowsDiffListA.length == 2)
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "hash1", inputRightHash = "hash2", diffs = List(
      ColumnsDiff(columnName = "id", values = List("1", "2")),
      ColumnsDiff(columnName = "name", values = List("Alice", "Bob"))
    ))))
    assert(rowsDiffListA.contains(RowsDiff(inputLeftHash = "hash3", inputRightHash = "hash4", diffs = List(
      ColumnsDiff(columnName = "id", values = List("3", "4")),
      ColumnsDiff(columnName = "name", values = List("Charlie", "David"))
    ))))
  }
}


import africa.absa.cps.io.IOHandler
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.native.JsonMethods.{compact, render}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.json4s.JsonDSL._

import java.io.File
import scala.reflect.io.Directory

class IOHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  val filePath = "src/test/resources/sample.parquet"

  override def afterAll(): Unit = {
    // Clean up
    new Directory(new File(filePath)).deleteRecursively()
  }

  test("test that sparkRead reads a Parquet file into a DataFrame") {
    val tmp: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    tmp.write.mode(SaveMode.Overwrite).format("parquet").save(filePath)

    val df: DataFrame = IOHandler.sparkRead(filePath)

    assert(df != null)
    assert(df.count() > 0)
  }

  test("test that dfWrite write a DataFrame to a Parquet file") {
    val df: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    IOHandler.dfWrite(filePath, df)

    val writtenDf: DataFrame = spark.read.parquet(filePath)
    assert(writtenDf.count() === 2)
    assert(writtenDf.columns === Array("id", "value"))

  }

  test("test that dfWrite write a DataFrame to a Parquet file") {
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

    IOHandler.jsonWrite(filePath, compact(render(metricsJson)))

    val source = scala.io.Source.fromFile("file.txt")
    val lines = try source.mkString finally source.close()
    assert(lines === compact(render(metricsJson)))
  }
}


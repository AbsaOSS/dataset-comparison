import africa.absa.cps.io.IOHandler
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.native.JsonMethods.{compact, render}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.json4s.JsonDSL._

import java.io.File
import scala.reflect.io.Directory

class IOHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter{

  implicit val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._
  val folder = "testoutput"
  val filePath: String = folder + "/sample.parquet"
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

    IOHandler.dfWrite(filePath, df)

    val writtenDf: DataFrame = spark.read.parquet(filePath)
    assert(writtenDf != null)
    assert(writtenDf.count() === 2)
    assert(writtenDf.columns === Array("id", "value"))

  }

  test("test that jsonWrite write a DataFrame to a Parquet file") {
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

    IOHandler.jsonWrite(jsonPath, compact(render(metricsJson)))

    val df = spark.read.json(jsonPath)
    val lines = df.collect().mkString("\n")
    assert(lines === compact(render(List(metricsJson))))
  }
}


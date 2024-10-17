import africa.absa.cps.writers.SparkWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.reflect.io.Directory

class SparkWriterTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  val filePath = "src/test/resources/sample.parquet"

  override def afterAll(): Unit = {
    // Clean up
    new Directory(new File(filePath)).deleteRecursively()
  }

  test("test that SparkWriter write a DataFrame to a Parquet file") {
    val df: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    SparkWriter.write(filePath, df)

    val writtenDf: DataFrame = spark.read.parquet(filePath)
    assert(writtenDf.count() === 2)
    assert(writtenDf.columns === Array("id", "value"))


  }
}

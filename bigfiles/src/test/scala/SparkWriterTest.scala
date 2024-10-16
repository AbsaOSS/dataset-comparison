import africa.absa.cps.writers.SparkWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

class SparkWriterTest extends AnyFunSuite with Matchers {

  val spark: SparkSession = SparkSession.builder()
    .appName("SparkWriterTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("test that SparkWriter write a DataFrame to a Parquet file") {
    val df: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val filePath = "src/test/resources/output.parquet"

    SparkWriter.write(filePath, df)

    val writtenDf: DataFrame = spark.read.parquet(filePath)
    assert(writtenDf.count() === 2)
    assert(writtenDf.columns === Array("id", "value"))

    // Clean up
    new File(filePath).delete()
  }
}

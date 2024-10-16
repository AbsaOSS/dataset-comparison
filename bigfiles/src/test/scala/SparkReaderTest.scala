import africa.absa.cps.readers.SparkReader
import africa.absa.cps.writers.SparkWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class SparkReaderTest extends AnyFunSuite with Matchers {

  val spark: SparkSession = SparkSession.builder()
    .appName("SparkReaderTest")
    .master("local[*]")
    .getOrCreate()

  test("SparkReader should read a Parquet file into a DataFrame") {
    val filePath = "src/test/resources/sample.parquet"
    val df: DataFrame = SparkReader.read(filePath, spark)

    assert(df != null)
    assert(df.count() > 0)
  }
}
